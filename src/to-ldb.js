import dtsApi from './data-totals';
import dlbApi from './data-ldb';
import { GAME_BTC } from './const';
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

const FORMULAS = [
  'up-verified_ok-TRUE-count',
  'up-verified_ok-FALSE-count',
  'up-verified_ok-N/A-count',
  'down-verified_ok-TRUE-count',
  'down-verified_ok-FALSE-count',
  'down-verified_ok-N/A-count',
  'verified_ok-TRUE-count-cont',
  'verified_ok-FALSE-count-cont',
  'verified_ok-N/A-count-cont',
  'verified_ok-TRUE-max-cont',
  'verified_ok-FALSE-max-cont',
  'verified_ok-N/A-max-cont',
];

const getLdbData = async (now) => {
  const { totals } = await dtsApi.getTotals(GAME_BTC);

  const bin = {}, sums = [];
  for (const total of totals) {
    const { stxAddr, formula } = total;
    if (stxAddr === 'all' || !FORMULAS.includes(formula)) continue;

    if (!isObject(bin[stxAddr])) bin[stxAddr] = {};
    bin[stxAddr][formula] = total;
  }
  for (const stxAddr in bin) {
    let sum = 0;
    if (isObject(bin[stxAddr]['up-verified_ok-TRUE-count'])) {
      sum += bin[stxAddr]['up-verified_ok-TRUE-count'].outcome;
    }
    if (isObject(bin[stxAddr]['down-verified_ok-TRUE-count'])) {
      sum += bin[stxAddr]['down-verified_ok-TRUE-count'].outcome;
    }
    sums.push({ stxAddr, sum });
  }
  sums.sort((a, b) => b.sum - a.sum);

  // Only 200 users who have highest outcome and allow in the leaderboard
  const N_USERS = 200, N_SPARE = 8;
  const stxAddrs = sums.slice(0, N_USERS + N_SPARE).map(sum => sum.stxAddr);
  const { users } = await dlbApi.getUsers(stxAddrs);
  const ldbUsers = users.filter(user => user.noInLbd !== true);

  if (ldbUsers.length < 200) {
    const nUsers = Math.ceil((N_USERS - ldbUsers.length) * 1.5);
    for (let i = N_USERS + N_SPARE; i < sums.length; i += nUsers) {
      const stxAddrs = sums.slice(i, i + nUsers).map(sum => sum.stxAddr);
      const { users } = await dlbApi.getUsers(stxAddrs);
      const moreUsers = users.filter(user => user.noInLbd !== true);
      ldbUsers.push(...moreUsers);

      if (ldbUsers.length >= 200) break;
    }
  }

  const data = { totals: {}, users: {}, createDate: now, updateDate: now };
  for (const user of ldbUsers.slice(0, N_USERS)) {
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

  const ldbData = await getLdbData(now);
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
