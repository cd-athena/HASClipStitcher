# HAS Clip Stitcher
Stitch media streaming session clips from CAdViSE and calculates MOS from P.1203 model

### Requirements
- Setup AWS access to dynamo db
- Install `python3` and Setup [ITU-T P.1203](https://github.com/itu-p1203/itu-p1203)
- Setup [FFMPEG](https://ffmpeg.org)

### Setup
- Add [CAdViSE](https://github.com/cd-athena/ppt) experiment ids in a json file under network shape name sample: `fluctuation.json`
- Add your dataset under `in` directory
- Modify following values in `app.js`
  - `region`
  - `TableName`
  - `displaySize`
  - `audioBitrate`
  - `segmentDuration`
  - `experimentDuration`
- Install node.js requirements `npm i`
- Set the correct node.js version `nvm use`

### Execute
```
node app.js
```