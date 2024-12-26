import dataApi from './data';
import dataTotalApi from './data-total';
import { PDG, SCS, NOT_FOUND_ERROR } from './const';
import {
  randomString, deriveTxInfo, getPredCorrect, getPredAnchorPrice, getPredTargetPrice,
} from './utils';

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-verified) starts on ${startDate.toISOString()}`);

  const { appBtcAddrs, preds } = await dataApi.getVerifyingPreds();
  console.log(`(${logKey}) got ${preds.length} verifying preds`);

  for (let i = 0; i < appBtcAddrs.length; i++) {
    const [appBtcAddr, pred] = [appBtcAddrs[i], preds[i]];

    let txInfo;
    try {
      txInfo = await dataApi.fetchTxInfo(pred.vTxId);
    } catch (error) {
      if (error.message !== NOT_FOUND_ERROR) {
        throw error; // server error, network error, throw.
      }
      if (Date.now() - pred.createDate < 60 * 60 * 1000) {
        console.log(`(${logKey}) ${pred.id} not found tx info, wait a bit more`);
        continue; // wait a bit more, maybe the api lacks behind.
      }

      console.log(`(${logKey}) ${pred.id} not found tx info, must reverify`);
      continue;
    }
    txInfo = deriveTxInfo(txInfo);
    if (txInfo.status === PDG) continue;

    const newPred = /** @type any */({ ...pred });
    newPred.vStatus = txInfo.status;
    if (newPred.vStatus === SCS) {
      newPred.correct = getPredCorrect(txInfo);
      if (['TRUE', 'FALSE'].includes(newPred.correct)) { // can be not available
        newPred.anchorPrice = getPredAnchorPrice(txInfo);
        newPred.targetPrice = getPredTargetPrice(txInfo);
      }
    }

    const udtRst = await dataApi.updatePred(appBtcAddr, newPred);
    console.log(`(${logKey}) ${pred.id} saved to Pred`);

    await dataTotalApi.udtTotVrd(appBtcAddr, udtRst.oldPred, udtRst.newPred);
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
