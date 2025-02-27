import dtsApi from './data-totals';
import dlbApi from './data-ldb';
import { GAME_BTC, N_LDB_USRS } from './const';
import { randomString, isObject } from './utils';

/*
storage-server-url/leaderboard/game/index.json
storage-server-url/leaderboard/game/<ts>.json
{
  data: {
    <ts> : {
      users: {
        stxAddr: {

        },
        ...
      },
      totals: {
        stxAddr: {

        },
        ...
      },
      createDate: ts,
      updateDate: ts,
    },
    .
    .
    .
  },
  prevTs: <prevTs>
}
*/

const getLdbData = async (game, now) => {
  const { totals: ldbTotals } = await dtsApi.getLdbTotals(game, N_LDB_USRS);

  const stxAddrs = ldbTotals.filter(t => t.stxAddr !== 'all')
    .sort((a, b) => b.outcome - a.outcome)
    .map(t => t.stxAddr);

  const { users } = await dlbApi.getUsers(stxAddrs);
  const ldbUsers = users.filter(user => user.noInLdb !== true).slice(0, N_LDB_USRS);
  // if players more than but ldbUsers less than N_LDB_USRS,
  //   try again with higher spare in getLdbTotals.

  const tkns = [];
  for (const user of ldbUsers) {
    const { stxAddr } = user;
    const kns = [
      `${stxAddr}-${game}-up-verified_ok-TRUE-count`,
      `${stxAddr}-${game}-up-verified_ok-FALSE-count`,
      `${stxAddr}-${game}-up-verified_ok-N/A-count`,
      `${stxAddr}-${game}-down-verified_ok-TRUE-count`,
      `${stxAddr}-${game}-down-verified_ok-FALSE-count`,
      `${stxAddr}-${game}-down-verified_ok-N/A-count`,
      `${stxAddr}-${game}-verified_ok-TRUE-count-cont`,
      `${stxAddr}-${game}-verified_ok-FALSE-count-cont`,
      `${stxAddr}-${game}-verified_ok-N/A-count-cont`,
      `${stxAddr}-${game}-verified_ok-TRUE-max-cont`,
      `${stxAddr}-${game}-verified_ok-FALSE-max-cont`,
      `${stxAddr}-${game}-verified_ok-N/A-max-cont`,
    ];
    tkns.push(...kns);
  }

  const { totals } = await dtsApi.getTotals(tkns);

  const bin = {};
  for (const total of totals) {
    const { stxAddr, formula } = total;
    if (!isObject(bin[stxAddr])) bin[stxAddr] = {};
    bin[stxAddr][formula] = total;
  }

  const data = { totals: {}, users: {}, createDate: now, updateDate: now };
  for (const user of ldbUsers) {
    const { stxAddr, username, avatar } = user;
    data.users[stxAddr] = { username, avatar };
    data.totals[stxAddr] = bin[stxAddr];
  }

  return data;
};

export const toLdb = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-ldb) starts on ${startDate.toISOString()}`);

  // Have LdbLog containing last processed updateDate and lastKeys.
  // Query those new preds to get games needed to regenerate leaderboards.

  const now = startDate.getTime();
  const iPath = `leaderboard/${GAME_BTC}/index.json`;
  const tPath = `leaderboard/${GAME_BTC}/${now}.json`;

  const nContent = { data: {}, prevTs: null };

  const iContent = await dlbApi.fetchStorage(iPath);
  console.log(`(${logKey}) fetched index from storage`);
  if (isObject(iContent) && isObject(iContent.data)) {
    const tss = Object.keys(iContent.data);
    if (tss.length > 0) {
      const stss = tss.map(ts => parseInt(ts, 10)).sort((a, b) => b - a);
      nContent.data[stss[0]] = iContent.data[stss[0]];
      if (stss.length > 1) nContent.prevTs = stss[1];
    }
  }

  const ldbData = await getLdbData(GAME_BTC, now);
  console.log(`(${logKey}) fetched ldb data`);

  nContent.data[ldbData.createDate] = ldbData;

  await dlbApi.updateStorage(tPath, nContent, 'public, max-age=31536000');
  await dlbApi.updateStorage(iPath, nContent, 'no-cache');
  console.log(`(${logKey}) saved to storage`);

  console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
};

const main = async () => {
  try {
    await toLdb();
    process.exit(0);
  } catch (e) {
    console.log(e);
    process.exit(1);
  }
};

main();
