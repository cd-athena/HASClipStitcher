const fs = require('fs')
const { spawnSync } = require('child_process')
const moment = require('moment')
const AWS = require('aws-sdk')
AWS.config.update({ region: 'eu-central-1' })
const dynamoDb = new AWS.DynamoDB.DocumentClient()

const networkShape = 'fluctuation'
const resultPath = 'result/'
const experimentIds = JSON.parse(fs.readFileSync(networkShape + '.json').toString())
fs.appendFileSync(resultPath + networkShape + '.csv', 'experimentId,sequenceTitle,playerABR,meanITUP1203,mediaTime,stallsTime,startUpTime\n')

experimentIds.forEach(async experimentId => {
  await new Promise((resolve, reject) => {
    let outputVideoFileName, outputAudioFileName, sequenceTitle, playerABR, outputPath
    let counter = -1
    let currentBitrate = 0
    let stallsTime = 0 // second
    let startUpTime = 0 // second
    let inputPath = 'in/'
    let mediaTime = 0
    let reInit = false
    const displaySize = '1280x720'
    const stallTolerance = 0.001
    const stitchedSegmentNames = []
    const audioBitrate = 128000
    const stallVideoPath = 'in/loading.mp4'
    const stallVideoDuration = 1.023 // second
    const segmentDuration = 2 // second
    const experimentDuration = 90 // second
    const stalling = []
    const ffmpegJobs = []
    const ITUP1203Args = [
      '-m', 'itu_p1203.extractor',
      '-m', 1
    ]

    const queryParams = {
      TableName: 'ppt-logs',
      IndexName: 'experimentId-index',
      KeyConditionExpression: '#experimentId = :experimentId',
      ExpressionAttributeNames: {
        '#experimentId': 'experimentId'
      },
      ExpressionAttributeValues: {
        ':experimentId': experimentId
      }
    }

    dynamoDb.query(queryParams, (error, data) => {
      if (error) {
        return reject(new Error(experimentId + ' Unable to query. ' + JSON.stringify(error, null, 2)))
      }

      if (data.Items.length > 0) {
        sequenceTitle = data.Items[0].title
        playerABR = data.Items[0].playerABR
        outputPath = resultPath + networkShape + '-' + sequenceTitle + '-' + playerABR + '-' + experimentId
        inputPath += data.Items[0].title + '/'
        fs.mkdirSync(outputPath)

        data.Items.sort((a, b) => new Date(a.time).valueOf() - new Date(b.time).valueOf())
        fs.writeFileSync(outputPath + '/CAdViSE' + '.json', JSON.stringify(data.Items))

        let startTime = 0
        data.Items.forEach(item => {
          if (item.name === 'manifest.mpd' && item.action === 'requesting') {
            startTime = moment(item.time)
          }
          if (item.name === 'playing' && item.action === 'event' && startUpTime === 0) {
            startUpTime = parseFloat((moment(item.time).diff(startTime) / 1000).toFixed(2))
            ++counter
            stalling.push([0, startUpTime])
          }
        })

        if (startUpTime < 1) {
          return reject(new Error(experimentId + ' Could not calculate the startup time'))
        }

        data.Items.forEach(item => {
          if (mediaTime + stallsTime + startUpTime < experimentDuration) {
            if (item.name !== 'manifest.mpd' && !item.name.includes(audioBitrate) && !item.name.includes('init') && item.action === 'requesting') {
              const [bitrate, segmentName] = item.name.split('/')
              if (!stitchedSegmentNames.includes(segmentName)) {
                stitchedSegmentNames.push(segmentName)
                mediaTime += segmentDuration

                if (bitrate !== currentBitrate || reInit) {
                  reInit = false
                  currentBitrate = bitrate
                  outputVideoFileName = outputPath + '/video-' + (++counter) + '.mp4'
                  outputAudioFileName = outputPath + '/audio-' + counter + '.mp4'
                  fs.appendFileSync(outputVideoFileName, fs.readFileSync(inputPath + bitrate + '/init.m4s'))
                  fs.appendFileSync(outputAudioFileName, fs.readFileSync(inputPath + audioBitrate + '/init.m4s'))
                }

                fs.appendFileSync(outputVideoFileName, fs.readFileSync(inputPath + bitrate + '/' + segmentName))
                fs.appendFileSync(outputAudioFileName, fs.readFileSync(inputPath + audioBitrate + '/' + segmentName))
              }
            } else if (item.action === 'event' && item.name === 'waiting') {
              const startStall = moment(item.time)

              let waitingFound
              data.Items.forEach(nextItem => {
                if (nextItem.id === item.id) {
                  waitingFound = true
                }
                if (waitingFound && nextItem.action === 'event' && nextItem.name === 'playing') {
                  let stallDuration = parseFloat((moment(nextItem.time).diff(startStall) / 1000).toFixed(3))
                  while (stallDuration + mediaTime + stallsTime + startUpTime > experimentDuration) {
                    stallDuration -= stallTolerance
                  }
                  reInit = true
                  stalling.push([mediaTime, stallDuration])
                  stallsTime += stallDuration
                  ++counter
                  waitingFound = false
                }
              })
            }
          }
        })

        let currentStallIndex = 0
        for (let i = 0; i < counter + 1; i++) {
          ffmpegJobs.push(new Promise((resolve, reject) => {
            if (fs.existsSync(outputPath + '/video-' + i + '.mp4')) {
              spawnSync('ffmpeg', [
                '-y',
                '-i', outputPath + '/video-' + i + '.mp4',
                '-i', outputPath + '/audio-' + i + '.mp4',
                '-c:v', 'copy',
                '-c:a', 'copy',
                outputPath + '/seg-' + i + '.mp4'
              ])

              fs.unlinkSync(outputPath + '/video-' + i + '.mp4')
              fs.unlinkSync(outputPath + '/audio-' + i + '.mp4')
              fs.appendFileSync(outputPath + '/list.txt', 'file \'seg-' + i + '.mp4\'\n')

              ITUP1203Args.push(outputPath + '/seg-' + i + '.mp4')
              resolve()
            } else {
              let stallDuration = stalling[currentStallIndex++][1]
              if (stallDuration <= stallVideoDuration) {
                spawnSync('ffmpeg', [
                  '-y',
                  '-i', stallVideoPath,
                  '-to', stallDuration,
                  outputPath + '/seg-' + i + '.mp4'
                ])
              } else {
                while (stallDuration > stallVideoDuration) {
                  fs.appendFileSync(outputPath + '/loading.txt', 'file \'../../' + stallVideoPath + '\'\n')
                  stallDuration -= stallVideoDuration
                }

                if (stallDuration > stallTolerance) {
                  spawnSync('ffmpeg', [
                    '-y',
                    '-i', stallVideoPath,
                    '-to', stallDuration,
                    outputPath + '/temp-loading.mp4'
                  ])

                  fs.appendFileSync(outputPath + '/loading.txt', 'file \'temp-loading.mp4\'\n')
                }

                spawnSync('ffmpeg', [
                  '-y',
                  '-f', 'concat',
                  '-safe', 0,
                  '-i', outputPath + '/loading.txt',
                  '-c', 'copy',
                  outputPath + '/seg-' + i + '.mp4']
                )

                if (stallDuration > stallTolerance) {
                  fs.unlinkSync(outputPath + '/temp-loading.mp4')
                }
                fs.unlinkSync(outputPath + '/loading.txt')
              }
              fs.appendFileSync(outputPath + '/list.txt', 'file \'seg-' + i + '.mp4\'\n')
              resolve()
            }
          }))
        }

        Promise.all(ffmpegJobs).then(() => {
          const ITUP1203Extractor = spawnSync('python3', ITUP1203Args)
          const ITUP1203Input = JSON.parse(ITUP1203Extractor.stdout.toString())

          ITUP1203Input.IGen.displaySize = displaySize
          ITUP1203Input.I23.stalling = stalling
          fs.writeFileSync(outputPath + '/ITUP1203Input.json', JSON.stringify(ITUP1203Input))

          const ITUP1203 = spawnSync('python3', [
            '-m', 'itu_p1203',
            outputPath + '/ITUP1203Input.json'
          ])
          const metrics = ITUP1203.stdout.toString()
          let metricsJson
          try {
            metricsJson = JSON.parse(metrics)
          } catch (exception) {
            console.log(experimentId)
            console.error(exception)
          }
          fs.writeFileSync(outputPath + '/ITUP1203.json', metrics)

          let total = 0
          Object.keys(metricsJson).forEach(segmentName => {
            total += metricsJson[segmentName].O46
          })

          const meanITUP1203 = total / Object.keys(metricsJson).length

          fs.appendFileSync(resultPath + networkShape + '.csv', experimentId + ',' + sequenceTitle + ',' +
            playerABR + ',' + meanITUP1203.toFixed(2) + ',' + mediaTime.toFixed(2) + ',' +
            stallsTime.toFixed(2) + ',' + startUpTime.toFixed(2) + '\n')

          spawnSync('ffmpeg', [
            '-y',
            '-f', 'concat',
            '-safe', 0,
            '-i', outputPath + '/list.txt',
            '-c', 'copy',
            resultPath + experimentId + '.mp4']
          )

          console.log(experimentId, 'done.')
          resolve()
        })
      } else {
        return reject(new Error(experimentId + ' Empty resultset from DDB'))
      }
    })
  })
})
