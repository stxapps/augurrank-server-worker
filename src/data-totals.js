import { Datastore } from '@google-cloud/datastore';

import {
  TOTAL, PRED_STATUS_CONFIRMED_OK, PRED_STATUS_VERIFIABLE, PRED_STATUS_VERIFYING,
  PRED_STATUS_VERIFIED_OK, PRED_STATUS_VERIFIED_ERROR, ALL,
} from './const';
import { sleep, isObject, sample, getPredStatus, isNotNullIn } from './utils';

const datastore = new Datastore();

const _udtTotCfd = async (oldPred, newPred) => {
  const doAdd = (
    getPredStatus(newPred) === PRED_STATUS_CONFIRMED_OK &&
    (oldPred === null || getPredStatus(oldPred) !== PRED_STATUS_CONFIRMED_OK)
  )
  if (!doAdd) return;

  const { stxAddr, game, value: predValue } = newPred;

  const keyNames = [
    `${stxAddr}-${game}-${predValue}-confirmed_ok-count`,
    `${stxAddr}-${game}-confirmed_ok-count-cont-day`,
    `${stxAddr}-${game}-confirmed_ok-max-cont-day`,
    `${stxAddr}-${predValue}-confirmed_ok-count`,
    `${stxAddr}-confirmed_ok-count-cont-day`,
    `${stxAddr}-confirmed_ok-max-cont-day`,
    `${game}-${predValue}-confirmed_ok-count`,
    `${game}-count-stxAddr`,
  ];
  const formulas = [
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'count-stxAddr',
  ];
  const keys = keyNames.map(kn => datastore.key([TOTAL, kn]));

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [_entities] = await transaction.get(keys);
    const entities = mapEntities(keyNames, _entities);

    const newEntities = [], now = Date.now();
    let keyName, key, entity, formula, total, isFirst, countCont;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 0);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, stxAddr, game, formula, 1, now, now);
      isFirst = true;
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 1);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= (18 + 24) * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, stxAddr, game, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 2);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < countCont) {
        [total.outcome, total.updateDate] = [countCont, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, stxAddr, game, formula, countCont, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 3);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, stxAddr, ALL, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 4);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= (18 + 24) * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, stxAddr, ALL, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 5);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < countCont) {
        [total.outcome, total.updateDate] = [countCont, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, stxAddr, ALL, formula, countCont, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 6);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, ALL, game, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    // We can know if this is a new user for this game by checking user+game exists.
    if (isFirst) {
      [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 7);
      if (isObject(entity)) {
        total = entityToTotal(entity);
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        total = newTotal(keyName, ALL, game, formula, 1, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    transaction.save(newEntities);
    await transaction.commit();
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const udtTotCfd = async (oldPred, newPred) => {
  const nTries = 3;
  for (let currentTry = 1; currentTry <= nTries; currentTry++) {
    try {
      await _udtTotCfd(oldPred, newPred);
      break;
    } catch (error) {
      if (currentTry < nTries) await sleep(sample([100, 200, 280, 350, 500]));
      else throw error;
    }
  }
};

const _udtTotVrd = async (oldPred, newPred) => {
  const doAdd = (
    getPredStatus(newPred) === PRED_STATUS_VERIFIED_OK &&
    (oldPred === null || getPredStatus(oldPred) !== PRED_STATUS_VERIFIED_OK)
  )
  if (!doAdd) return;

  const { stxAddr, game, value: predValue, correct: predCorrect } = newPred;

  const keyNames = [
    `${stxAddr}-${game}-${predValue}-confirmed_ok-count`,
    `${stxAddr}-${predValue}-confirmed_ok-count`,
    `${game}-${predValue}-confirmed_ok-count`,
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
    `${game}-${predValue}-verified_ok-${predCorrect}-count`
  ];
  const formulas = [
    `${predValue}-confirmed_ok-count`,
    `${predValue}-confirmed_ok-count`,
    `${predValue}-confirmed_ok-count`,
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
    `${predValue}-verified_ok-${predCorrect}-count`
  ];
  const keys = keyNames.map(kn => datastore.key([TOTAL, kn]));

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [_entities] = await transaction.get(keys);
    const entities = mapEntities(keyNames, _entities);

    const newEntities = [], now = Date.now();
    let keyName, key, entity, formula, total, countCont;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 0);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome - 1, now];
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 1);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome - 1, now];
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 2);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome - 1, now];
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 3);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, stxAddr, game, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 4);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
      total.updateDate = now;
    } else {
      const outcome = predCorrect === 'TRUE' ? 1 : 0;
      total = newTotal(keyName, stxAddr, game, formula, outcome, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    if (predCorrect === 'TRUE') countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 5);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      total.outcome = predCorrect === 'FALSE' ? total.outcome + 1 : 0;
      total.updateDate = now;
    } else {
      const outcome = predCorrect === 'FALSE' ? 1 : 0;
      total = newTotal(keyName, stxAddr, game, formula, outcome, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    if (predCorrect === 'FALSE') countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 6);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      total.outcome = predCorrect === 'N/A' ? total.outcome + 1 : 0;
      total.updateDate = now;
    } else {
      const outcome = predCorrect === 'N/A' ? 1 : 0;
      total = newTotal(keyName, stxAddr, game, formula, outcome, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    if (predCorrect === 'N/A') countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 7);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < countCont) {
        [total.outcome, total.updateDate] = [countCont, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, stxAddr, game, formula, countCont, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 8);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, stxAddr, ALL, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 9);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
      total.updateDate = now;
    } else {
      const outcome = predCorrect === 'TRUE' ? 1 : 0;
      total = newTotal(keyName, stxAddr, ALL, formula, outcome, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    if (predCorrect === 'TRUE') countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 10);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      total.outcome = predCorrect === 'FALSE' ? total.outcome + 1 : 0;
      total.updateDate = now;
    } else {
      const outcome = predCorrect === 'FALSE' ? 1 : 0;
      total = newTotal(keyName, stxAddr, ALL, formula, outcome, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    if (predCorrect === 'FALSE') countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 11);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      total.outcome = predCorrect === 'N/A' ? total.outcome + 1 : 0;
      total.updateDate = now;
    } else {
      const outcome = predCorrect === 'N/A' ? 1 : 0;
      total = newTotal(keyName, stxAddr, ALL, formula, outcome, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    if (predCorrect === 'N/A') countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 12);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < countCont) {
        [total.outcome, total.updateDate] = [countCont, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, stxAddr, ALL, formula, countCont, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 13);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, ALL, game, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    transaction.save(newEntities);
    await transaction.commit();
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const udtTotVrd = async (oldPred, newPred) => {
  const nTries = 3;
  for (let currentTry = 1; currentTry <= nTries; currentTry++) {
    try {
      await _udtTotVrd(oldPred, newPred);
      break;
    } catch (error) {
      if (currentTry < nTries) await sleep(sample([100, 200, 280, 350, 500]));
      else throw error;
    }
  }
};

const genUserTotals = (preds) => {
  const sPreds = [...preds].sort((a, b) => a.createDate - b.createDate);

  const totals = {}, now = Date.now();
  for (const pred of sPreds) {
    const { stxAddr, game, value: predValue, correct: predCorrect, createDate } = pred;

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
        total = totals[keyName];
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, game, formula, 1, now, now);
      }

      [keyName, formula] = [keyNames[3], formulas[3]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, ALL, formula, 1, now, now);
      }
    } else if (status === PRED_STATUS_VERIFIED_OK) {
      [keyName, formula] = [keyNames[6], formulas[6]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, game, formula, 1, now, now);
      }

      [keyName, formula] = [keyNames[7], formulas[7]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
        total.updateDate = now;
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
        total.updateDate = now;
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
        total.updateDate = now;
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
          [total.outcome, total.updateDate] = [countCont, now];
        }
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, game, formula, countCont, now, now);
      }

      [keyName, formula] = [keyNames[11], formulas[11]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        totals[keyName] = newTotal(keyName, stxAddr, ALL, formula, 1, now, now);
      }

      [keyName, formula] = [keyNames[12], formulas[12]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
        total.updateDate = now;
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
        total.updateDate = now;
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
        total.updateDate = now;
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
          [total.outcome, total.updateDate] = [countCont, now];
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

        total.updateDate = now;
      } else {
        total = newTotal(keyName, stxAddr, game, formula, 1, now, now, createDate);
        totals[keyName] = total;
      }
      countCont = total.outcome;

      [keyName, formula] = [keyNames[2], formulas[2]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (total.outcome < countCont) {
          [total.outcome, total.updateDate] = [countCont, now];
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

        total.updateDate = now;
      } else {
        total = newTotal(keyName, stxAddr, ALL, formula, 1, now, now, createDate);
        totals[keyName] = total;
      }
      countCont = total.outcome;

      [keyName, formula] = [keyNames[5], formulas[5]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        if (total.outcome < countCont) {
          [total.outcome, total.updateDate] = [countCont, now];
        }
      } else {
        total = newTotal(keyName, stxAddr, ALL, formula, countCont, now, now);
        totals[keyName] = total;
      }
    }
  }

  return totals;
};

const genGameTotals = (preds) => {
  const sPreds = [...preds].sort((a, b) => a.createDate - b.createDate);

  const totals = {}, goneAddrs = [], now = Date.now();
  for (const pred of sPreds) {
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

    let keyName, formula, total;
    if (status === PRED_STATUS_CONFIRMED_OK) {
      [keyName, formula] = [keyNames[0], formulas[0]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        totals[keyName] = newTotal(keyName, ALL, game, formula, 1, now, now);
      }
    } else if (status === PRED_STATUS_VERIFIED_OK) {
      [keyName, formula] = [keyNames[1], formulas[1]];
      if (isObject(totals[keyName])) {
        total = totals[keyName];
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
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
          total = totals[keyName];
          [total.outcome, total.updateDate] = [total.outcome + 1, now];
        } else {
          totals[keyName] = newTotal(keyName, ALL, game, formula, 1, now, now);
        }
        goneAddrs.push(stxAddr);
      }
    }
  }

  return totals;
};

const updateTotals = async (totals) => {
  const keyNames = totals.map(t => t.keyName);
  const keys = keyNames.map(k => datastore.key([TOTAL, k]));

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [_entities] = await transaction.get(keys);
    const entities = mapEntities(keyNames, _entities);

    const newEntities = [];
    for (let i = 0; i < keyNames.length; i++) {
      const [total, key, entity] = [totals[i], keys[i], entities[i]];

      let newTotal;
      if (isObject(entity)) {
        newTotal = entityToTotal(entity);
        if (
          newTotal.keyName !== total.keyName ||
          newTotal.stxAddr !== total.stxAddr ||
          newTotal.game !== total.game ||
          newTotal.formula !== total.formula ||
          ('anchor' in newTotal && !('anchor' in total)) ||
          (!('anchor' in newTotal) && 'anchor' in total)
        ) {
          throw new Error(`Invalid total: ${JSON.stringify(total)}`);
        }
        [newTotal.outcome, newTotal.updateDate] = [total.outcome, total.updateDate];
        if ('anchor' in total && 'anchor' in newTotal) newTotal.anchor = total.anchor;
      } else {
        newTotal = total;
      }

      newEntities.push({ key, data: totalToEntityData(newTotal) });
    }

    transaction.save(newEntities);
    await transaction.commit();
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const getAt = (keyNames, keys, entities, formulas, i) => {
  return [keyNames[i], keys[i], entities[i], formulas[i]];
};

const newTotal = (
  keyName, stxAddr, game, formula, outcome, createDate, updateDate, anchor = null
) => {
  const total = {
    keyName, stxAddr, game, formula, outcome, createDate, updateDate,
  };
  if (anchor !== null) total.anchor = anchor;
  return total;
};

const totalToEntityData = (total) => {
  const data = [
    { name: 'stxAddr', value: total.stxAddr },
    { name: 'game', value: total.game },
    { name: 'formula', value: total.formula },
    { name: 'outcome', value: total.outcome, excludeFromIndexes: true },
    { name: 'createDate', value: new Date(total.createDate) },
    { name: 'updateDate', value: new Date(total.updateDate) },
  ];
  if ('anchor' in total) {
    data.push({ name: 'anchor', value: total.anchor, excludeFromIndexes: true });
  }
  return data;
};

const entityToTotal = (entity) => {
  const total = {
    keyName: entity[datastore.KEY].name,
    stxAddr: entity.stxAddr,
    game: entity.game,
    formula: entity.formula,
    outcome: entity.outcome,
    createDate: entity.createDate.getTime(),
    updateDate: entity.updateDate.getTime(),
  };
  if (isNotNullIn(entity, 'anchor')) total.anchor = entity.anchor;

  return total;
};

const mapEntities = (keyNames, _entities) => {
  const knToEtt = {};
  for (const ett of _entities) {
    if (!isObject(ett)) continue;
    knToEtt[ett[datastore.KEY].name] = ett;
  }

  const entities = [];
  for (const keyName of keyNames) {
    const ett = knToEtt[keyName];
    entities.push(isObject(ett) ? ett : null);
  }
  return entities;
};

const data = { udtTotCfd, udtTotVrd, genUserTotals, genGameTotals, updateTotals };

export default data;
