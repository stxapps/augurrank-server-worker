import dataApi from './data';
import dataTotalApi from './data-total';
import {
  GAME_BTC, GAME_BTC_LEAD_BURN_HEIGHT, PDG, SCS, ABT_BY_NF, NOT_FOUND_ERROR,
} from './const';
import { randomString, deriveTxInfo, getPredSeq } from './utils';

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-confirmed) starts on ${startDate.toISOString()}`);

  const { appBtcAddrs, preds } = await dataApi.getUnconfirmedPreds();
  console.log(`(${logKey}) got ${preds.length} unconfirmed preds`);

  for (let i = 0; i < appBtcAddrs.length; i++) {
    const [appBtcAddr, pred] = [appBtcAddrs[i], preds[i]];

    let txInfo;
    try {
      txInfo = await dataApi.fetchTxInfo(pred.cTxId);
    } catch (error) {
      if (error.message !== NOT_FOUND_ERROR) {
        throw error; // server error, network error, throw.
      }
      if (Date.now() - pred.createDate < 60 * 60 * 1000) {
        console.log(`(${logKey}) ${pred.id} not found tx info, wait a bit more`);
        continue; // wait a bit more, maybe the api lacks behind.
      }

      // Not in mempool anymore like cannot confirm i.e. wrong nonce, not enough fee
      txInfo = { tx_id: pred.cTxId, status: ABT_BY_NF };
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

    const udtRst = await dataApi.updatePred(appBtcAddr, newPred);
    console.log(`(${logKey}) ${pred.id} saved to Pred`);

    await dataTotalApi.udtTotCfd(appBtcAddr, udtRst.oldPred, udtRst.newPred);
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
