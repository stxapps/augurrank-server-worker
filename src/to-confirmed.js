import dataApi from './data';
import dtsApi from './data-totals';
import {
  GAME_BTC, GAME_BTC_LEAD_BURN_HEIGHT, PDG, SCS, ABT_BY_NF, ERR_NOT_FOUND,
} from './const';
import { randomString, deriveTxInfo, getPredSeq } from './utils';

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-confirmed) starts on ${startDate.toISOString()}`);

  const { preds } = await dataApi.getUnconfirmedPreds();
  console.log(`(${logKey}) got ${preds.length} unconfirmed preds`);

  for (const pred of preds) {
    let txInfo;
    try {
      txInfo = await dataApi.fetchTxInfo(pred.cTxId);
    } catch (error) {
      if (error.message !== ERR_NOT_FOUND) {
        throw error; // server error, network error, throw.
      }
      if (Date.now() - pred.createDate < 60 * 60 * 1000) {
        console.log(`(${logKey}) ${pred.id} not found tx info, wait a bit more`);
        continue; // wait a bit more, maybe the api lacks behind.
      }

      // Not in mempool anymore like cannot confirm i.e. wrong nonce, not enough fee
      txInfo = { tx_id: pred.cTxId, tx_status: ABT_BY_NF };
    }
    txInfo = deriveTxInfo(txInfo);
    if (txInfo.status === PDG) continue;

    const newPred = /** @type any */({ ...pred });
    newPred.cStatus = txInfo.status;
    if (txInfo.status !== ABT_BY_NF) {
      newPred.anchorHeight = txInfo.height;
      newPred.anchorBurnHeight = txInfo.burnHeight;
    }
    if (txInfo.status === SCS) {
      if (pred.game === GAME_BTC) {
        newPred.seq = getPredSeq(txInfo);
        newPred.targetBurnHeight = txInfo.burnHeight + GAME_BTC_LEAD_BURN_HEIGHT;
      }
    }

    const udtRst = await dataApi.updatePred(newPred);
    console.log(`(${logKey}) ${pred.id} saved to Pred`);

    await dtsApi.udtTotCfd(udtRst.oldPred, udtRst.newPred);
    console.log(`(${logKey}) ${pred.id} saved to Total`);
  }

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
