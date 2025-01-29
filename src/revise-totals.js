import dataApi from './data';
import dataTotalApi from './data-total';
import {
  PRED_STATUS_CONFIRMED_OK, PRED_STATUS_VERIFIABLE, PRED_STATUS_VERIFYING,
  PRED_STATUS_VERIFIED_OK, PRED_STATUS_VERIFIED_ERROR, ALL,
} from './const';
import { randomString, getPredStatus, isObject } from './utils';

const newTotal = dataTotalApi.newTotal;

const getUserTotals = async (logKey, stxAddr) => {
  const { preds } = await dataApi.queryPreds(stxAddr, null);
  console.log(`(${logKey}) got ${preds.length} preds`);

  preds.sort((a, b) => a.createDate - b.createDate);

  const totals = {}, now = Date.now();
  for (const pred of preds) {
    const { game, value: predValue, correct: predCorrect, createDate } = pred;

    const keyNames = [
      `${stxAddr}-${game}-${predValue}-confirmed_ok-count`,
      `${stxAddr}-${game}-confirmed_ok-count-cont-day`,
      `${stxAddr}-${game}-confirmed_ok-max-cont-day`,
      `${stxAddr}-${predValue}-confirmed_ok-count`,
      `${stxAddr}-confirmed_ok-count-cont-day`,
      `${stxAddr}-confirmed_ok-max-cont-day`,
      `${stxAddr}-${game}-${predValue}-verified_ok-${predCorrect}-count`,
      `${stxAddr}-${game}-verified_ok-TRUE-count-cont`,
      `${stxAddr}-${game}-verified_ok-FALSE-count-cont`,
      `${stxAddr}-${game}-verified_ok-N/A-count-cont`,
      `${stxAddr}-${game}-verified_ok-${predCorrect}-max-cont`,
      `${stxAddr}-${predValue}-verified_ok-${predCorrect}-count`,
      `${stxAddr}-verified_ok-TRUE-count-cont`,
      `${stxAddr}-verified_ok-FALSE-count-cont`,
      `${stxAddr}-verified_ok-N/A-count-cont`,
      `${stxAddr}-verified_ok-${predCorrect}-max-cont`,
    ];
    const formulas = [
      `${predValue}-confirmed_ok-count`,
      'confirmed_ok-count-cont-day',
      'confirmed_ok-max-cont-day',
      `${predValue}-confirmed_ok-count`,
      'confirmed_ok-count-cont-day',
      'confirmed_ok-max-cont-day',
      `${predValue}-verified_ok-${predCorrect}-count`,
      `verified_ok-TRUE-count-cont`,
      `verified_ok-FALSE-count-cont`,
      `verified_ok-N/A-count-cont`,
      `verified_ok-${predCorrect}-max-cont`,
      `${predValue}-verified_ok-${predCorrect}-count`,
      `verified_ok-TRUE-count-cont`,
      `verified_ok-FALSE-count-cont`,
      `verified_ok-N/A-count-cont`,
      `verified_ok-${predCorrect}-max-cont`,
    ];

    let status = getPredStatus(pred);
    if ([PRED_STATUS_VERIFIABLE, PRED_STATUS_VERIFYING].includes(status)) {
      status = PRED_STATUS_CONFIRMED_OK;
    }

    let keyName, formula, total, countCont;
    if (status === PRED_STATUS_CONFIRMED_OK) {
      [keyName, formula] = [keyNames[0], formulas[0]];
      if (isObject(totals[keyName])) {
        totals[keyName].outcome += 1;
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, game, formula, 1, now, now);
      }

      [keyName, formula] = [keyNames[3], formulas[3]];
      if (isObject(totals[keyName])) {
        totals[keyName].outcome += 1;
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, ALL, formula, 1, now, now);
      }
    } else if (status === PRED_STATUS_VERIFIED_OK) {
      [keyName, formula] = [keyNames[6], formulas[6]];
      if (isObject(totals[keyName])) {
        totals[keyName].outcome += 1;
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, game, formula, 1, now, now);
      }

      [keyName, formula] = [keyNames[7], formulas[7]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
      } else {
        const outcome = predCorrect === 'TRUE' ? 1 : 0;
        total = newTotal(keyName, stxAddr, game, formula, outcome, now, now);
        totals[keyName] = total;
      }
      if (predCorrect === 'TRUE') countCont = total.outcome;

      [keyName, formula] = [keyNames[8], formulas[8]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'FALSE' ? total.outcome + 1 : 0;
      } else {
        const outcome = predCorrect === 'FALSE' ? 1 : 0;
        total = newTotal(keyName, stxAddr, game, formula, outcome, now, now);
        totals[keyName] = total;
      }
      if (predCorrect === 'FALSE') countCont = total.outcome;

      [keyName, formula] = [keyNames[9], formulas[9]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'N/A' ? total.outcome + 1 : 0;
      } else {
        const outcome = predCorrect === 'N/A' ? 1 : 0;
        total = newTotal(keyName, stxAddr, game, formula, outcome, now, now);
        totals[keyName] = total
      }
      if (predCorrect === 'N/A') countCont = total.outcome;

      [keyName, formula] = [keyNames[10], formulas[10]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (total.outcome < countCont) {
          total.outcome = countCont;
        }
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, game, formula, countCont, now, now);
      }

      [keyName, formula] = [keyNames[11], formulas[11]];
      if (isObject(totals[keyName])) {
        totals[keyName].outcome += 1;
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, ALL, formula, 1, now, now);
      }

      [keyName, formula] = [keyNames[12], formulas[12]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
      } else {
        const outcome = predCorrect === 'TRUE' ? 1 : 0;
        total = newTotal(keyName, stxAddr, ALL, formula, outcome, now, now);
        totals[keyName] = total;
      }
      if (predCorrect === 'TRUE') countCont = total.outcome;

      [keyName, formula] = [keyNames[13], formulas[13]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'FALSE' ? total.outcome + 1 : 0;
      } else {
        const outcome = predCorrect === 'FALSE' ? 1 : 0;
        total = newTotal(keyName, stxAddr, ALL, formula, outcome, now, now);
        totals[keyName] = total;
      }
      if (predCorrect === 'FALSE') countCont = total.outcome;

      [keyName, formula] = [keyNames[14], formulas[14]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'N/A' ? total.outcome + 1 : 0;
      } else {
        const outcome = predCorrect === 'N/A' ? 1 : 0;
        total = newTotal(keyName, stxAddr, ALL, formula, outcome, now, now);
        totals[keyName] = total;
      }
      if (predCorrect === 'N/A') countCont = total.outcome;

      [keyName, formula] = [keyNames[15], formulas[15]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (total.outcome < countCont) {
          total.outcome = countCont;
        }
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, ALL, formula, countCont, now, now);
      }
    }

    if ([
      PRED_STATUS_CONFIRMED_OK, PRED_STATUS_VERIFIED_OK, PRED_STATUS_VERIFIED_ERROR,
    ].includes(status)) {
      [keyName, formula] = [keyNames[1], formulas[1]]
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (createDate - total.anchor <= (18 + 24) * 60 * 60 * 1000) {
          [total.outcome, total.anchor] = [total.outcome + 1, createDate];
        } else {
          [total.outcome, total.anchor] = [1, createDate];
        }
      } else {
        total = newTotal(keyName, stxAddr, game, formula, 1, now, now, createDate);
        totals[keyName] = total;
      }
      countCont = total.outcome;

      [keyName, formula] = [keyNames[2], formulas[2]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (total.outcome < countCont) {
          total.outcome = countCont;
        }
      } else {
        total = newTotal(keyName, stxAddr, game, formula, countCont, now, now);
        totals[keyName] = total;
      }

      [keyName, formula] = [keyNames[4], formulas[4]]
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (createDate - total.anchor <= (18 + 24) * 60 * 60 * 1000) {
          [total.outcome, total.anchor] = [total.outcome + 1, createDate];
        } else {
          [total.outcome, total.anchor] = [1, createDate];
        }
      } else {
        total = newTotal(keyName, stxAddr, ALL, formula, 1, now, now, createDate);
        totals[keyName] = total;
      }
      countCont = total.outcome;

      [keyName, formula] = [keyNames[5], formulas[5]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (total.outcome < countCont) {
          total.outcome = countCont;
        }
      } else {
        total = newTotal(keyName, stxAddr, ALL, formula, countCont, now, now);
        totals[keyName] = total;
      }
    }
  }

  return totals;
};

const getGameTotals = async (logKey, game) => {
  const { preds } = await dataApi.queryPreds(null, game);
  console.log(`(${logKey}) got ${preds.length} preds`);

  preds.sort((a, b) => a.createDate - b.createDate);

  const totals = {}, goneAddrs = [], now = Date.now();
  for (const pred of preds) {
    const { stxAddr, game, value: predValue, correct: predCorrect } = pred;

    const keyNames = [
      `${game}-${predValue}-confirmed_ok-count`,
      `${game}-${predValue}-verified_ok-${predCorrect}-count`,
      `${game}-count-stxAddr`,
    ];
    const formulas = [
      `${predValue}-confirmed_ok-count`,
      `${predValue}-verified_ok-${predCorrect}-count`,
      'count-stxAddr',
    ];

    let status = getPredStatus(pred);
    if ([PRED_STATUS_VERIFIABLE, PRED_STATUS_VERIFYING].includes(status)) {
      status = PRED_STATUS_CONFIRMED_OK;
    }

    let keyName, formula;
    if (status === PRED_STATUS_CONFIRMED_OK) {
      [keyName, formula] = [keyNames[0], formulas[0]];
      if (isObject(totals[keyName])) {
        totals[keyName].outcome += 1;
      } else {
        totals[keyName] = newTotal(keyName, ALL, game, formula, 1, now, now);
      }
    } else if (status === PRED_STATUS_VERIFIED_OK) {
      [keyName, formula] = [keyNames[1], formulas[1]];
      if (isObject(totals[keyName])) {
        totals[keyName].outcome += 1;
      } else {
        totals[keyName] = newTotal(keyName, ALL, game, formula, 1, now, now);
      }
    }

    if ([
      PRED_STATUS_CONFIRMED_OK, PRED_STATUS_VERIFIED_OK, PRED_STATUS_VERIFIED_ERROR,
    ].includes(status)) {
      if (!goneAddrs.includes(stxAddr)) {
        [keyName, formula] = [keyNames[2], formulas[2]];
        if (isObject(totals[keyName])) {
          totals[keyName].outcome += 1;
        } else {
          totals[keyName] = newTotal(keyName, ALL, game, formula, 1, now, now);
        }
        goneAddrs.push(stxAddr);
      }
    }
  }

  return totals;
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
      console.log(`${t.keyName}: ${t.outcome}`);
    }
  }
};

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-totals) starts on ${startDate.toISOString()}`);

  const stxAddr = 'SP18EDVDZRXYWG6Z0CB4J3Q7R37164ACY6TBSVB9K';
  const game = 'GameBtc';

  //const uTotals = await getUserTotals(logKey, stxAddr);
  //printTotals(uTotals);

  const gTotals = await getGameTotals(logKey, game);
  printTotals(gTotals);

  //await dataApi.updateTotals([...uTotals, gTotals]);

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
