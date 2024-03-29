const fs = require('fs')
const { spawnSync } = require('child_process')
const moment = require('moment')
const AWS = require('aws-sdk')
AWS.config.update({ region: 'eu-central-1' })
const dynamoDb = new AWS.DynamoDB.DocumentClient()

const networkShape = 'network'
const resultPath = 'result/'
const generateVideo = false
const experimentIds = JSON.parse(fs.readFileSync(networkShape + '.json').toString())
fs.appendFileSync(resultPath + networkShape + '.csv', 'experimentId,sequenceTitle,playerABR,meanITUP1203,mediaTime,stallsTime,startUpTime,qualitySwitch,averageBitrate\n')

experimentIds.forEach(async experimentId => {
  await new Promise((resolve, reject) => {
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

    let resultset
    let items = [];
    (async () => {
      do {
        if (resultset && resultset.LastEvaluatedKey) {
          queryParams.ExclusiveStartKey = resultset.LastEvaluatedKey
        }
        try {
          const data = await dynamoDb.query(queryParams).promise()
          resultset = data
          items = items.concat(data.Items)
        } catch (e) {
          return reject(new Error(experimentId + ' Unable to query. ' + JSON.stringify(e, null, 2)))
        }
      } while (resultset.LastEvaluatedKey)

      if (items.length > 0) {
        const sequenceTitle = items[0].title

        const clients = {}
        items.forEach(item => {
          if (!clients[item.playerABR]) {
            clients[item.playerABR] = []
          }
          clients[item.playerABR].push(item)
        })

        Object.keys(clients).forEach(playerABR => {
          let outputVideoFileName, outputAudioFileName
          let counter = -1
          let currentBitrate = 0
          let stallsTime = 0 // second
          let startUpTime = 0 // second
          let mediaTime = 0
          let reInit = false
          let qualitySwitchNumber = 0
          let inputPath = 'in/'
          const displaySize = '1280x720'
          const stallTolerance = 0.001
          const audioBitrate = 128000
          const stallVideoPath = 'in/loading.mp4'
          const stallVideoDuration = 1.023 // second
          const segmentDuration = 4 // second
          const frameRate = 24
          const experimentDuration = 180 // second
          const bitrates = []
          const stitchedSegmentNames = []
          const stalling = []
          const ffmpegJobs = []
          const ITUP1203Args = [
            '-m', 'itu_p1203.extractor',
            '-m', 1
          ]

          const outputPath = resultPath + networkShape + '-' + sequenceTitle + '-' + playerABR + '-' + experimentId
          inputPath += sequenceTitle + '/'
          fs.mkdirSync(outputPath)

          clients[playerABR].sort((a, b) => new Date(a.time).valueOf() - new Date(b.time).valueOf())
          fs.writeFileSync(outputPath + '/CAdViSE' + '.json', JSON.stringify(clients[playerABR]))

          let startTime = 0
          clients[playerABR].forEach(item => {
            if (item.name === 'manifest.mpd' && item.action === 'requesting') {
              startTime = moment(item.time)
            }
            if (item.name === 'playing' && item.action === 'event' && startUpTime === 0) {
              startUpTime = parseFloat((moment(item.time).diff(startTime) / 1000).toFixed(2))
              ++counter
              stalling.push([0, startUpTime])
            }
          })

          clients[playerABR].forEach(item => {
            if (mediaTime + stallsTime + startUpTime < experimentDuration) {
              if (item.name !== 'manifest.mpd' && !item.name.includes(audioBitrate) && !item.name.includes('init') && item.action === 'requesting') {
                const [bitrate, segmentName] = item.name.split('/')
                if (!stitchedSegmentNames.includes(segmentName)) {
                  stitchedSegmentNames.push(segmentName)
                  mediaTime += segmentDuration

                  bitrates.push(parseInt(bitrate))

                  if (bitrate !== currentBitrate && currentBitrate !== 0) {
                    qualitySwitchNumber++
                  }

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
                clients[playerABR].forEach(nextItem => {
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
              } else if (generateVideo) {
                const stallDuration = stalling[currentStallIndex++][1]
                if (stallDuration <= stallVideoDuration) {
                  spawnSync('ffmpeg', [
                    '-y',
                    '-i', stallVideoPath,
                    '-to', stallDuration,
                    outputPath + '/stitchedLoading.mp4'
                  ])
                } else {
                  let leftStallDuration = stallDuration
                  while (leftStallDuration > stallVideoDuration) {
                    fs.appendFileSync(outputPath + '/loading.txt', 'file \'../../' + stallVideoPath + '\'\n')
                    leftStallDuration -= stallVideoDuration
                  }

                  if (leftStallDuration > stallTolerance) {
                    spawnSync('ffmpeg', [
                      '-y',
                      '-i', stallVideoPath,
                      '-to', leftStallDuration,
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
                    outputPath + '/stitchedLoading.mp4'
                  ])

                  if (leftStallDuration > stallTolerance) {
                    fs.unlinkSync(outputPath + '/temp-loading.mp4')
                  }
                  fs.unlinkSync(outputPath + '/loading.txt')
                }

                if (fs.existsSync(outputPath + '/seg-' + (i - 1) + '.mp4')) {
                  const ffprobeLastSegment = spawnSync('ffprobe', [
                    '-v', 'error',
                    '-select_streams', 'v:0',
                    '-show_entries', 'stream=width,height,sample_aspect_ratio,display_aspect_ratio',
                    '-of', 'json',
                    outputPath + '/seg-' + (i - 1) + '.mp4'
                  ])

                  const lastSegmentProperties = JSON.parse(ffprobeLastSegment.stdout.toString())
                  const resolution = lastSegmentProperties.streams[0].width + 'x' + lastSegmentProperties.streams[0].height
                  const sar = lastSegmentProperties.streams[0].sample_aspect_ratio
                  const dar = lastSegmentProperties.streams[0].display_aspect_ratio

                  spawnSync('ffmpeg', [
                    '-y',
                    '-i', outputPath + '/stitchedLoading.mp4',
                    '-r', frameRate,
                    '-s', resolution,
                    '-vf', 'setsar=' + sar + ',setdar=' + dar,
                    outputPath + '/adjustedLoading.mp4'
                  ])

                  fs.unlinkSync(outputPath + '/stitchedLoading.mp4')

                  spawnSync('ffmpeg', [
                    '-sseof', '-3',
                    '-i', outputPath + '/seg-' + (i - 1) + '.mp4',
                    '-update', 1,
                    '-q:v', 1,
                    outputPath + '/lastSegmentLastFrame.png'
                  ])

                  spawnSync('ffmpeg', [
                    '-y',
                    '-framerate', frameRate,
                    '-loop', 1,
                    '-i', outputPath + '/lastSegmentLastFrame.png',
                    '-i', outputPath + '/adjustedLoading.mp4',
                    '-filter_complex', '[1]format=argb,colorchannelmixer=aa=0.5[ol];[0][ol]overlay',
                    '-t', stallDuration,
                    outputPath + '/seg-' + i + '.mp4'
                  ])

                  fs.unlinkSync(outputPath + '/adjustedLoading.mp4')
                  fs.unlinkSync(outputPath + '/lastSegmentLastFrame.png')
                } else {
                  fs.renameSync(outputPath + '/stitchedLoading.mp4', outputPath + '/seg-' + i + '.mp4')
                }

                fs.appendFileSync(outputPath + '/list.txt', 'file \'seg-' + i + '.mp4\'\n')
              }
              resolve()
            }))
          }

          Promise.all(ffmpegJobs).then(() => {
            const ITUP1203Extractor = spawnSync('python3', ITUP1203Args)
            const extractorOutput = ITUP1203Extractor.stdout.toString()

            try {
              const ITUP1203Input = JSON.parse(extractorOutput)
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
                stallsTime.toFixed(2) + ',' + startUpTime.toFixed(2) + ',' + qualitySwitchNumber +
                ',' + bitrates.reduce(function (p, c, i, a) { return p + (c / a.length) }, 0).toFixed(2) + '\n')

              if (generateVideo) {
                spawnSync('ffmpeg', [
                  '-y',
                  '-f', 'concat',
                  '-safe', 0,
                  '-i', outputPath + '/list.txt',
                  '-c', 'copy',
                  resultPath + experimentId + '-' + playerABR + '.mp4']
                )
              }

              console.log(experimentId, playerABR, 'done.')
            } catch (e) {
              console.error(experimentId, playerABR, ITUP1203Extractor.stderr.toString(), e.toString())
            }
            resolve()
          })
        })
      } else {
        return reject(new Error(experimentId + ' Empty resultset from DDB'))
      }
    })()
  })
})
