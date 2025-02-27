import dataApi from './data';
import dtsApi from './data-totals';
import dlbApi from './data-ldb';
import dprApi from './data-plyr';

import { randomString, isObject, isFldStr } from './utils';

/*
storage-server-url/player/index.json
storage-server-url/player/<ts>.json
{
  data: {
    username: '',
    avatar: '',
    bio: '',
    stats: {

    },
    preds: [

    ],
  },
  prevFName: null | '<ts>.json',
}
*/
const newContent = (noPlyrPg = false) => {
  return {
    data: {
      username: null, avatar: null, bio: null, noPlyrPg, stats: {}, preds: [],
    },
    prevFName: null,
  };
};

const joinPreds = (preds, newPreds) => {
  const res = [], ids = [];
  for (const pred of [...newPreds, ...preds]) {
    if (ids.includes(pred.id)) continue;
    res.push(pred);
    ids.push(pred.id);
  }

  res.sort((a, b) => b.updateDate - a.updateDate);
  return res;
};

export const toPlyr = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-plyr) starts on ${startDate.toISOString()}`);

  let uUdtDt = 0, uKeys = [], pUdtDt = 0, pKeys = [];
  const log = await dprApi.getLastestPlyrLog();
  console.log(`(${logKey}) Got ${log ? '1' : '0'} latest PlyrLog entity`);
  if (isObject(log)) {
    [uUdtDt, uKeys, pUdtDt, pKeys] = [log.uUdtDt, log.uKeys, log.pUdtDt, log.pKeys];
  }

  const stxAddrs = {};
  const nwLg = {
    uUdtDt, uKeys: [...uKeys], pUdtDt, pKeys: [...pKeys],
    createDate: startDate.getTime(), updateDate: startDate.getTime(),
  };

  const { users } = await dlbApi.getUpdatedUsers(uUdtDt);
  console.log(`(${logKey}) Got ${users.length} User entities`);
  for (const user of users) {
    if (user.updateDate === uUdtDt && uKeys.includes(user.stxAddr)) continue;

    if (!isObject(stxAddrs[user.stxAddr])) {
      stxAddrs[user.stxAddr] = { user: null, preds: [] };
    }
    stxAddrs[user.stxAddr].user = user;

    if (user.updateDate > nwLg.uUdtDt) {
      [nwLg.uUdtDt, nwLg.uKeys] = [user.updateDate, [user.stxAddr]];
    } else if (user.updateDate === nwLg.uUdtDt) {
      nwLg.uKeys.push(user.stxAddr);
    }
  }

  const { preds } = await dataApi.getVerifiedPreds(pUdtDt);
  console.log(`(${logKey}) Got ${preds.length} Pred entities`);
  for (const pred of preds) {
    if (pred.updateDate === pUdtDt && pKeys.includes(pred.id)) continue;

    if (!isObject(stxAddrs[pred.stxAddr])) {
      stxAddrs[pred.stxAddr] = { user: null, preds: [] };
    }
    stxAddrs[pred.stxAddr].preds.push(pred);

    if (pred.updateDate > nwLg.pUdtDt) {
      [nwLg.pUdtDt, nwLg.pKeys] = [pred.updateDate, [pred.id]];
    } else if (pred.updateDate === nwLg.pUdtDt) {
      nwLg.pKeys.push(pred.id);
    }
  }

  const lnt = Object.keys(stxAddrs).length;
  console.log(`(${logKey}) Got ${lnt} stale stx addresses`);
  if (lnt === 0) {
    console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
    return;
  }

  for (const [stxAddr, { user, preds }] of Object.entries(stxAddrs)) {
    const dir = `player/${stxAddr}`, path = `${dir}/index.json`;
    const content = await dlbApi.fetchStorage(path);

    if (preds.length === 0 && !isObject(content)) continue;

    let nwCnt = content, nwUsr = user;
    if (!isObject(nwCnt)) {
      nwCnt = newContent();

      if (!isObject(nwUsr)) {
        const { users } = await dlbApi.getUsers([stxAddr]);
        nwUsr = users[0];
      }
    }
    if (isObject(nwUsr)) {
      if (nwUsr.noPlyrPg === true) {
        await dlbApi.deleteStorage(dir);

        nwCnt = newContent(true);
        await dlbApi.updateStorage(path, nwCnt, 'no-cache');
        continue;
      }

      nwCnt.data.username = isFldStr(nwUsr.username) ? nwUsr.username : null;
      nwCnt.data.avatar = isFldStr(nwUsr.avatar) ? nwUsr.avatar : null;
      nwCnt.data.bio = isFldStr(nwUsr.bio) ? nwUsr.bio : null;
    }
    if (preds.length > 0) {
      const stats = await dtsApi.getStats(stxAddr);
      nwCnt.data.stats = stats;

      nwCnt.data.preds = joinPreds(nwCnt.data.preds, preds);
    }

    if (nwCnt.data.preds.length > 60) {
      // split files
    }
    await dlbApi.updateStorage(path, nwCnt, 'no-cache');
  }
  console.log(`(${logKey}) Updated storage`);

  await dprApi.savePlyrLog(nwLg);
  console.log(`(${logKey}) Saved latest PlyrLog`);

  console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
};

const main = async () => {
  try {
    await toPlyr();
    process.exit(0);
  } catch (e) {
    console.log(e);
    process.exit(1);
  }
};

main();
