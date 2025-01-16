import { Datastore } from '@google-cloud/datastore';

import { TOTAL, PRED_STATUS_CONFIRMED_OK, PRED_STATUS_VERIFIED_OK, ALL } from './const';
import { sleep, isObject, sample, getPredStatus, isNotNullIn } from './utils';

const datastore = new Datastore();

const _udtTotCfd = async (appBtcAddr, oldPred, newPred) => {
  const doAdd = (
    getPredStatus(newPred) === PRED_STATUS_CONFIRMED_OK &&
    (oldPred === null || getPredStatus(oldPred) !== PRED_STATUS_CONFIRMED_OK)
  )
  if (!doAdd) return;

  const { game, value: predValue } = newPred;

  const keyNames = [
    `${appBtcAddr}-${game}-${predValue}-confirmed_ok-count`,
    `${appBtcAddr}-${game}-confirmed_ok-count-cont-day`,
    `${appBtcAddr}-${game}-confirmed_ok-max-cont-day`,
    `${appBtcAddr}-${predValue}-confirmed_ok-count`,
    `${appBtcAddr}-confirmed_ok-count-cont-day`,
    `${appBtcAddr}-confirmed_ok-max-cont-day`,
    `${game}-${predValue}-confirmed_ok-count`,
    `${game}-count-appBtcAddr`,
  ];
  const formulas = [
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'count-appBtcAddr',
  ];
  const keys = keyNames.map(kn => datastore.key([TOTAL, kn]));

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [_entities] = await transaction.get(keys);
    const entities = mapEntities(keyNames, _entities);

    const newEntities = [], now = Date.now();
    let keyName, key, entity, formula, total, isFirst, contDay;

    [keyName, key, entity, formula] = [keyNames[0], keys[0], entities[0], formulas[0]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, appBtcAddr, game, formula, 1, now, now);
      isFirst = true;
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = [keyNames[1], keys[1], entities[1], formulas[1]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= 24 * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, appBtcAddr, game, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    contDay = total.outcome;

    [keyName, key, entity, formula] = [keyNames[2], keys[2], entities[2], formulas[2]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < contDay) {
        [total.outcome, total.updateDate] = [contDay, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, appBtcAddr, game, formula, contDay, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = [keyNames[3], keys[3], entities[3], formulas[3]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, appBtcAddr, ALL, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = [keyNames[4], keys[4], entities[4], formulas[4]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= 24 * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, appBtcAddr, ALL, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    contDay = total.outcome;

    [keyName, key, entity, formula] = [keyNames[5], keys[5], entities[5], formulas[5]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < contDay) {
        [total.outcome, total.updateDate] = [contDay, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, appBtcAddr, ALL, formula, contDay, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = [keyNames[6], keys[6], entities[6], formulas[6]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, ALL, game, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    // We can know if this is a new user for this game by checking user+game exists.
    if (isFirst) {
      [keyName, key, entity, formula] = [keyNames[7], keys[7], entities[7], formulas[7]];
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

const udtTotCfd = async (appBtcAddr, oldPred, newPred) => {
  const nTries = 3;
  for (let currentTry = 1; currentTry <= nTries; currentTry++) {
    try {
      await _udtTotCfd(appBtcAddr, oldPred, newPred);
      break;
    } catch (error) {
      if (currentTry < nTries) await sleep(sample([100, 200, 280, 350, 500]));
      else throw error;
    }
  }
};

const _udtTotVrd = async (appBtcAddr, oldPred, newPred) => {
  const doAdd = (
    getPredStatus(newPred) === PRED_STATUS_VERIFIED_OK &&
    (oldPred === null || getPredStatus(oldPred) !== PRED_STATUS_VERIFIED_OK)
  )
  if (!doAdd) return;

  const { game, value: predValue, correct: predCorrect } = newPred;

  const keyNames = [
    `${appBtcAddr}-${game}-${predValue}-confirmed_ok-count`,
    `${appBtcAddr}-${predValue}-confirmed_ok-count`,
    `${game}-${predValue}-confirmed_ok-count`,
  ];
  const formulas = [
    `${predValue}-confirmed_ok-count`,
    `${predValue}-confirmed_ok-count`,
    `${predValue}-confirmed_ok-count`,
  ];
  if (['TRUE', 'FALSE'].includes(predCorrect)) {
    keyNames.push(...[
      `${appBtcAddr}-${game}-${predValue}-verified_ok-${predCorrect}-count`,
      `${appBtcAddr}-${game}-verified_ok-TRUE-count-cont`,
      `${appBtcAddr}-${game}-verified_ok-FALSE-count-cont`,
      `${appBtcAddr}-${game}-verified_ok-${predCorrect}-max-cont`,
      `${appBtcAddr}-${predValue}-verified_ok-${predCorrect}-count`,
      `${appBtcAddr}-verified_ok-TRUE-count-cont`,
      `${appBtcAddr}-verified_ok-FALSE-count-cont`,
      `${appBtcAddr}-verified_ok-${predCorrect}-max-cont`,
      `${game}-${predValue}-verified_ok-${predCorrect}-count`
    ]);
    formulas.push(...[
      `${predValue}-verified_ok-${predCorrect}-count`,
      `verified_ok-TRUE-count-cont`,
      `verified_ok-FALSE-count-cont`,
      `verified_ok-${predCorrect}-max-cont`,
      `${predValue}-verified_ok-${predCorrect}-count`,
      `verified_ok-TRUE-count-cont`,
      `verified_ok-FALSE-count-cont`,
      `verified_ok-${predCorrect}-max-cont`,
      `${predValue}-verified_ok-${predCorrect}-count`
    ]);
  }
  const keys = keyNames.map(kn => datastore.key([TOTAL, kn]));

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [_entities] = await transaction.get(keys);
    const entities = mapEntities(keyNames, _entities);

    const newEntities = [], now = Date.now();
    let keyName, key, entity, formula, total, contDay;

    [keyName, key, entity, formula] = [keyNames[0], keys[0], entities[0], formulas[0]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome - 1, now];
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = [keyNames[1], keys[1], entities[1], formulas[1]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome - 1, now];
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = [keyNames[2], keys[2], entities[2], formulas[2]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome - 1, now];
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    if (['TRUE', 'FALSE'].includes(predCorrect)) {
      [keyName, key, entity, formula] = [keyNames[3], keys[3], entities[3], formulas[3]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        total = newTotal(keyName, appBtcAddr, game, formula, 1, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });

      [keyName, key, entity, formula] = [keyNames[4], keys[4], entities[4], formulas[4]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
        total.updateDate = now;
      } else {
        const outcome = predCorrect === 'TRUE' ? 1 : 0;
        total = newTotal(keyName, appBtcAddr, game, formula, outcome, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });
      if (predCorrect === 'TRUE') contDay = total.outcome;

      [keyName, key, entity, formula] = [keyNames[5], keys[5], entities[5], formulas[5]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        total.outcome = predCorrect === 'FALSE' ? total.outcome + 1 : 0;
        total.updateDate = now;
      } else {
        const outcome = predCorrect === 'FALSE' ? 1 : 0;
        total = newTotal(keyName, appBtcAddr, game, formula, outcome, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });
      if (predCorrect === 'FALSE') contDay = total.outcome;

      [keyName, key, entity, formula] = [keyNames[6], keys[6], entities[6], formulas[6]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        if (total.outcome < contDay) {
          [total.outcome, total.updateDate] = [contDay, now];
          newEntities.push({ key, data: totalToEntityData(total) });
        }
      } else {
        total = newTotal(keyName, appBtcAddr, game, formula, contDay, now, now);
        newEntities.push({ key, data: totalToEntityData(total) });
      }

      [keyName, key, entity, formula] = [keyNames[7], keys[7], entities[7], formulas[7]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        total = newTotal(keyName, appBtcAddr, ALL, formula, 1, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });

      [keyName, key, entity, formula] = [keyNames[8], keys[8], entities[8], formulas[8]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        total.outcome = predCorrect === 'TRUE' ? total.outcome + 1 : 0;
        total.updateDate = now;
      } else {
        const outcome = predCorrect === 'TRUE' ? 1 : 0;
        total = newTotal(keyName, appBtcAddr, ALL, formula, outcome, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });
      if (predCorrect === 'TRUE') contDay = total.outcome;

      [keyName, key, entity, formula] = [keyNames[9], keys[9], entities[9], formulas[9]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        total.outcome = predCorrect === 'FALSE' ? total.outcome + 1 : 0;
        total.updateDate = now;
      } else {
        const outcome = predCorrect === 'FALSE' ? 1 : 0;
        total = newTotal(keyName, appBtcAddr, ALL, formula, outcome, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });
      if (predCorrect === 'FALSE') contDay = total.outcome;

      [keyName, key] = [keyNames[10], keys[10]];
      [entity, formula] = [entities[10], formulas[10]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        if (total.outcome < contDay) {
          [total.outcome, total.updateDate] = [contDay, now];
          newEntities.push({ key, data: totalToEntityData(total) });
        }
      } else {
        total = newTotal(keyName, appBtcAddr, ALL, formula, contDay, now, now);
        newEntities.push({ key, data: totalToEntityData(total) });
      }

      [keyName, key] = [keyNames[11], keys[11]];
      [entity, formula] = [entities[11], formulas[11]];
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

const udtTotVrd = async (appBtcAddr, oldPred, newPred) => {
  const nTries = 3;
  for (let currentTry = 1; currentTry <= nTries; currentTry++) {
    try {
      await _udtTotVrd(appBtcAddr, oldPred, newPred);
      break;
    } catch (error) {
      if (currentTry < nTries) await sleep(sample([100, 200, 280, 350, 500]));
      else throw error;
    }
  }
};

const newTotal = (
  keyName, appBtcAddr, game, formula, outcome, createDate, updateDate, anchor = null
) => {
  const total = {
    keyName, appBtcAddr, game, formula, outcome, createDate, updateDate,
  };
  if (anchor !== null) total.anchor = anchor;
  return total;
};

const totalToEntityData = (total) => {
  const data = [
    { name: 'appBtcAddr', value: total.appBtcAddr },
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
    appBtcAddr: entity.appBtcAddr,
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

const data = { udtTotCfd, udtTotVrd };

export default data;
