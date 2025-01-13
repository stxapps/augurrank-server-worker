import { Datastore, PropertyFilter, and } from '@google-cloud/datastore';

import { PRED, SCS, NOT_FOUND_ERROR } from './const';
import { isObject, isNumber, mergePreds, isNotNullIn } from './utils';

const datastore = new Datastore();

const fetchBurnHeight = async () => {
  const res = await fetch('https://api.hiro.so/extended');
  if (!res.ok) {
    throw new Error(res.statusText);
  }
  const obj = await res.json();
  const height = obj.chain_tip.burn_block_height;
  return height;
};

const bHToH = {};
const fetchHeight = async (burnHeight) => {
  if (isNumber(bHToH[burnHeight])) return bHToH[burnHeight];

  let res = await fetch(`https://api.hiro.so/extended/v2/burn-blocks/${burnHeight}`);
  if (res.status === 404) {
    bHToH[burnHeight] = -1;
    return -1;
  }
  if (!res.ok) {
    throw new Error(res.statusText);
  }
  let obj = await res.json();
  const hash = obj.stacks_blocks[obj.stacks_blocks.length - 1];

  res = await fetch(`https://api.hiro.so/extended/v2/blocks/${hash}`);
  if (!res.ok) {
    throw new Error(res.statusText);
  }
  obj = await res.json();
  const height = obj.height;

  bHToH[burnHeight] = height;
  return height;
};

const fetchTxInfo = async (txId) => {
  const res = await fetch(`https://api.hiro.so/extended/v1/tx/{txId}`);
  if (res.status === 404) {
    throw new Error(NOT_FOUND_ERROR);
  }
  if (!res.ok) {
    throw new Error(res.statusText);
  }
  const obj = await res.json();
  return obj;
};

const getUnconfirmedPreds = async () => {
  const query = datastore.createQuery(PRED);
  query.filter(new PropertyFilter('cStatus', '=', null));
  query.order('createDate', { descending: false }); // for cont-day's anchor in Total
  query.limit(40);

  const [entities] = await datastore.runQuery(query);

  const appBtcAddrs = [], preds = [];
  if (Array.isArray(entities)) {
    for (const entity of entities) {
      if (!isObject(entity)) continue;

      const appBtcAddr = entity.appBtcAddr;
      const pred = entityToPred(entity);
      appBtcAddrs.push(appBtcAddr);
      preds.push(pred);
    }
  }

  return { appBtcAddrs, preds };
};

const getVerifiablePreds = async (burnHeight) => {
  const query = datastore.createQuery(PRED);
  query.filter(and([
    new PropertyFilter('vTxId', '=', null),
    new PropertyFilter('targetBurnHeight', '<', burnHeight),
  ]));
  query.order('targetBurnHeight', { descending: false }); // inequality must be first
  query.order('createDate', { descending: false }); // for cont-day's anchor in Total
  query.limit(40);

  const [entities] = await datastore.runQuery(query);

  const appBtcAddrs = [], preds = [];
  if (Array.isArray(entities)) {
    for (const entity of entities) {
      if (!isObject(entity)) continue;

      const appBtcAddr = entity.appBtcAddr;
      const pred = entityToPred(entity);
      appBtcAddrs.push(appBtcAddr);
      preds.push(pred);
    }
  }

  return { appBtcAddrs, preds };
};

const getVerifyingPreds = async () => {
  const query = datastore.createQuery(PRED);
  query.filter(new PropertyFilter('vStatus', '=', null));
  query.order('createDate', { descending: false }); // for cont-day's anchor in Total
  query.limit(40);

  const [entities] = await datastore.runQuery(query);

  const appBtcAddrs = [], preds = [];
  if (Array.isArray(entities)) {
    for (const entity of entities) {
      if (!isObject(entity)) continue;

      const appBtcAddr = entity.appBtcAddr;
      const pred = entityToPred(entity);
      appBtcAddrs.push(appBtcAddr);
      preds.push(pred);
    }
  }

  return { appBtcAddrs, preds };
};

const updatePred = async (appBtcAddr, pred) => {
  const predKey = datastore.key([PRED, pred.id]);

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [oldEntity] = await transaction.get(predKey);
    if (!isObject(oldEntity)) {
      await transaction.rollback();
      throw new Error('Invalid oldEntity');
    }
    if (oldEntity.appBtcAddr !== appBtcAddr) {
      await transaction.rollback();
      throw new Error('Invalid appBtcAddr');
    }
    const oldPred = entityToPred(oldEntity);

    const newPred = mergePreds(oldPred, pred);
    const newEntity = predToEntityData(appBtcAddr, newPred);

    transaction.save(newEntity);
    await transaction.commit();
    return { oldPred, newPred };
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const predToEntityData = (appBtcAddr, pred) => {
  // Need cStatus, vTxId, and vStatus for Datastore queries in worker.
  let isCstRqd = false, isVxiRqd = false, isVstRqd = false;

  const data = [
    { name: 'game', value: pred.game },
    { name: 'contract', value: pred.contract },
    { name: 'value', value: pred.value },
    { name: 'createDate', value: new Date(pred.createDate) },
    { name: 'updateDate', value: new Date(pred.updateDate) },
    { name: 'stxAddr', value: pred.stxAddr },
  ];
  if ('cTxId' in pred) {
    data.push({ name: 'cTxId', value: pred.cTxId });
    isCstRqd = true;
  }
  if ('pStatus' in pred) data.push({ name: 'pStatus', value: pred.pStatus });
  if ('cStatus' in pred) {
    data.push({ name: 'cStatus', value: pred.cStatus });
    if (pred.cStatus === SCS) isVxiRqd = true;
  } else if (isCstRqd) {
    data.push({ name: 'cStatus', value: null });
  }
  if ('anchorHeight' in pred) {
    data.push({ name: 'anchorHeight', value: pred.anchorHeight });
  }
  if ('anchorBurnHeight' in pred) {
    data.push({ name: 'anchorBurnHeight', value: pred.anchorBurnHeight });
  }
  if ('seq' in pred) {
    data.push({ name: 'seq', value: pred.seq });
  }
  if ('targetBurnHeight' in pred) {
    data.push({ name: 'targetBurnHeight', value: pred.targetBurnHeight });
  }
  if ('vTxId' in pred) {
    data.push({ name: 'vTxId', value: pred.vTxId });
    isVstRqd = true;
  } else if (isVxiRqd) {
    data.push({ name: 'vTxId', value: null });
  }
  if ('targetHeight' in pred) {
    data.push({ name: 'targetHeight', value: pred.targetHeight });
  }
  if ('vStatus' in pred) {
    data.push({ name: 'vStatus', value: pred.vStatus });
  } else if (isVstRqd) {
    data.push({ name: 'vStatus', value: null });
  }
  if ('anchorPrice' in pred) {
    data.push({ name: 'anchorPrice', value: pred.anchorPrice });
  }
  if ('targetPrice' in pred) {
    data.push({ name: 'targetPrice', value: pred.targetPrice });
  }
  if ('correct' in pred) {
    data.push({ name: 'correct', value: pred.correct });
  }

  // IMPORTANT: pred doesn't have appBtcAddr, but predEntity must have it!
  data.push({ name: 'appBtcAddr', value: appBtcAddr });

  return data;
};

const entityToPred = (entity) => {
  const pred = {
    id: entity[datastore.KEY].name,
    game: entity.game,
    contract: entity.contract,
    value: entity.value,
    createDate: entity.createDate.getTime(),
    updateDate: entity.updateDate.getTime(),
    stxAddr: entity.stxAddr,
  };
  if (isNotNullIn(entity, 'cTxId')) pred.cTxId = entity.cTxId;
  if (isNotNullIn(entity, 'pStatus')) pred.pStatus = entity.pStatus;
  if (isNotNullIn(entity, 'cStatus')) pred.cStatus = entity.cStatus;
  if (isNotNullIn(entity, 'anchorHeight')) pred.anchorHeight = entity.anchorHeight;
  if (isNotNullIn(entity, 'anchorBurnHeight')) {
    pred.anchorBurnHeight = entity.anchorBurnHeight;
  }
  if (isNotNullIn(entity, 'seq')) pred.seq = entity.seq;
  if (isNotNullIn(entity, 'targetBurnHeight')) {
    pred.targetBurnHeight = entity.targetBurnHeight;
  }
  if (isNotNullIn(entity, 'vTxId')) pred.vTxId = entity.vTxId;
  if (isNotNullIn(entity, 'targetHeight')) pred.targetHeight = entity.targetHeight;
  if (isNotNullIn(entity, 'vStatus')) pred.vStatus = entity.vStatus;
  if (isNotNullIn(entity, 'anchorPrice')) pred.anchorPrice = entity.anchorPrice;
  if (isNotNullIn(entity, 'targetPrice')) pred.targetPrice = entity.targetPrice;
  if (isNotNullIn(entity, 'correct')) pred.correct = entity.correct;

  return pred;
};

const data = {
  fetchBurnHeight, fetchHeight, fetchTxInfo, getUnconfirmedPreds, getVerifiablePreds,
  getVerifyingPreds, updatePred,
};

export default data;
