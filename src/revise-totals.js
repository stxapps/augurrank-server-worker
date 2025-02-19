import dataApi from './data';
import dtsApi from './data-totals';
import { randomString } from './utils';

const reviseUserTotals = async (logKey) => {
  const stxAddr = 'SP18E...SVB9K';

  const { preds } = await dataApi.queryPreds(stxAddr, null);
  console.log(`(${logKey}) got ${preds.length} preds`);

  const totals = dtsApi.genUserTotals(preds);
  printTotals(totals);

  //await dtsApi.updateTotals(Object.values(totals));
};

const reviseGameTotals = async (logKey) => {
  const game = 'GameBtc';

  const { preds } = await dataApi.queryPreds(null, game);
  console.log(`(${logKey}) got ${preds.length} preds`);

  const totals = dtsApi.genGameTotals(preds);
  printTotals(totals);

  //await dtsApi.updateTotals(Object.values(totals));
};

const printTotals = (totals) => {
  const tts = Object.values(totals);

  const ttspg = {};
  for (const t of tts) {
    if (t.game in ttspg) continue;
    ttspg[t.game] = [];
  }
  for (const t of tts) {
    ttspg[t.game].push(t);
  }
  for (const g in ttspg) {
    ttspg[g].sort((a, b) => {
      return a.formula.localeCompare(b.formula);
    });

    for (const t of ttspg[g]) {
      let txt = `${t.keyName}: ${t.outcome}`;
      if ('anchor' in t) txt += ` (${t.anchor})`;
      console.log(txt);
    }
  }
};

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-totals) starts on ${startDate.toISOString()}`);

  await reviseUserTotals(logKey);
  //await reviseGameTotals(logKey);

  console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
};

const main = async () => {
  try {
    await _main();
    process.exit(0);
  } catch (e) {
    console.log(e);
    process.exit(1);
  }
};

main();
