import {
  PRED_STATUS_INIT, PRED_STATUS_IN_MEMPOOL, PRED_STATUS_PUT_OK,
  PRED_STATUS_PUT_ERROR, PRED_STATUS_CONFIRMED_OK, PRED_STATUS_CONFIRMED_ERROR,
  PRED_STATUS_VERIFIABLE, PRED_STATUS_VERIFYING, PRED_STATUS_VERIFIED_OK,
  PRED_STATUS_VERIFIED_ERROR, PDG, SCS,
} from './const';

export const sleep = (ms) => {
  return new Promise(resolve => setTimeout(resolve, ms));
};

export const isObject = (val) => {
  return typeof val === 'object' && val !== null;
};

export const isString = (val) => {
  return typeof val === 'string';
};

export const isNumber = (val) => {
  return typeof val === 'number' && isFinite(val);
};

export const sample = (arr) => {
  return arr[Math.floor(Math.random() * arr.length)];
};

export const randomString = (length) => {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  const charactersLength = characters.length;

  let result = '';
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
};

export const getStatusText = (res) => {
  return `${res.status} ${res.statusText}`;
};

export const mergePreds = (...preds) => {
  const bin = {
    updateDate: null,
    pStatus: { scs: null, updg: null },
    cStatus: { scs: null, updg: null },
    vStatus: { scs: null, updg: null },
  };

  let newPred = {};
  for (const pred of preds) {
    if (!isObject(pred)) continue;

    if (isNumber(pred.updateDate)) {
      if (!isNumber(bin.updateDate) || pred.updateDate > bin.updateDate) {
        bin.updateDate = pred.updateDate;
      }
    }
    if (isString(pred.pStatus)) {
      if (pred.pStatus === SCS) bin.pStatus.scs = pred.pStatus;
      else if (pred.pStatus !== PDG) bin.pStatus.updg = pred.pStatus;
    }
    if (isString(pred.cStatus)) {
      if (pred.cStatus === SCS) bin.cStatus.scs = pred.cStatus;
      else if (pred.cStatus !== PDG) bin.cStatus.updg = pred.cStatus;
    }
    if (isString(pred.vStatus)) {
      if (pred.vStatus === SCS) bin.vStatus.scs = pred.vStatus;
      else if (pred.vStatus !== PDG) bin.vStatus.updg = pred.vStatus;
    }

    newPred = { ...newPred, ...pred };
  }

  if (isNumber(bin.updateDate)) newPred.updateDate = bin.updateDate;

  if (isString(bin.pStatus.scs)) newPred.pStatus = bin.pStatus.scs;
  else if (isString(bin.pStatus.updg)) newPred.pStatus = bin.pStatus.updg;

  if (isString(bin.cStatus.scs)) newPred.cStatus = bin.cStatus.scs;
  else if (isString(bin.cStatus.updg)) newPred.cStatus = bin.cStatus.updg;

  if (isString(bin.vStatus.scs)) newPred.vStatus = bin.vStatus.scs;
  else if (isString(bin.vStatus.updg)) newPred.vStatus = bin.vStatus.updg;

  return newPred;
};

export const getPredStatus = (pred, burnHeight = null) => {
  if ('pStatus' in pred && ![PDG, SCS].includes(pred.pStatus)) {
    return PRED_STATUS_PUT_ERROR;
  }
  if ('cStatus' in pred && ![PDG, SCS].includes(pred.cStatus)) {
    return PRED_STATUS_CONFIRMED_ERROR;
  }
  if ('vStatus' in pred && ![PDG, SCS].includes(pred.vStatus)) {
    return PRED_STATUS_VERIFIED_ERROR;
  }

  if (pred.vStatus === SCS) return PRED_STATUS_VERIFIED_OK;
  if ('vTxId' in pred) return PRED_STATUS_VERIFYING;
  if (pred.cStatus === SCS) {
    if (
      isNumber(pred.targetBurnHeight) &&
      isNumber(burnHeight) &&
      pred.targetBurnHeight < burnHeight
    ) {
      return PRED_STATUS_VERIFIABLE;
    }
    return PRED_STATUS_CONFIRMED_OK;
  }
  if (pred.pStatus === SCS) return PRED_STATUS_PUT_OK;
  if ('cTxId' in pred) return PRED_STATUS_IN_MEMPOOL;
  return PRED_STATUS_INIT;
};

export const deriveTxInfo = (txInfo) => {
  const obj = {
    txId: txInfo.tx_id,
    status: txInfo.tx_status,
    height: null,
    burnHeight: null,
    result: null,
    vls: null,
  };
  if (isNumber(txInfo.block_height)) obj.height = txInfo.block_height;
  if (isNumber(txInfo.burn_block_height)) obj.burnHeight = txInfo.burn_block_height;
  if (isObject(txInfo.tx_result) && isString(txInfo.tx_result.repr)) {
    obj.result = txInfo.tx_result.repr;
  }
  if (Array.isArray(txInfo.events)) {
    obj.vls = [];
    for (const evt of txInfo.events) {
      try {
        obj.vls.push(evt.contract_log.value.repr);
      } catch (error) {
        // might be other event types.
      }
    }
  }
  return obj;
};

const getPredNumber = (regex, txInfo) => {
  try {
    const match = txInfo.result.match(regex);
    if (match) {
      const nmbr = parseInt(match[1]);
      if (isNumber(nmbr)) return nmbr;
    }
  } catch (error) {
    // txInfo.result might not be string.
  }

  return -1;
};

export const getPredSeq = (txInfo) => {
  const regex = /\(seq\s+u(\d+)\)/; // (ok (tuple (seq u532)))
  return getPredNumber(regex, txInfo);
};

export const getPredCorrect = (txInfo) => {
  const regex = /\(correct\s+"(TRUE|FALSE|N\/A)"\)/; // (ok (tuple (correct "N/A")))
  try {
    const match = txInfo.result.match(regex);
    if (match) {
      const value = match[1];
      if (['TRUE', 'FALSE', 'N/A'].includes(value)) return value;
    }
  } catch (error) {
    // txInfo.result might not be string.
  }

  return null;
};

export const getPredAnchorPrice = (txInfo) => {
  const regex = /\(anchor-price\s+u(\d+)\)/; // (ok (tuple (anchor-price u99999)))
  return getPredNumber(regex, txInfo);
};

export const getPredTargetPrice = (txInfo) => {
  const regex = /\(target-price\s+u(\d+)\)/; // (ok (tuple (anchor-price u99999)))
  return getPredNumber(regex, txInfo);
};

export const isNotNullIn = (entity, key) => {
  return key in entity && entity[key] !== null;
};
