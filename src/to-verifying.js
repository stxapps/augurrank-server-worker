import {
  makeContractCall, broadcastTransaction, PostConditionMode, Cl, Pc,
} from '@stacks/transactions';

import dataApi from './data';
import { CONTRACT_ADDR } from './const';
import { randomString, mergePreds } from './utils';
import { SENDER_KEY } from './keys';

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker(to-verifying) starts on ${startDate.toISOString()}`);

  const burnHeight = await dataApi.fetchBurnHeight();
  const { appBtcAddrs, preds } = await dataApi.getVerifiablePreds(burnHeight);
  console.log(`(${logKey}) got ${preds.length} verifiable preds`);

  const condition1 = Pc.principal(CONTRACT_ADDR).willNotSendAsset();
  const condition2 = Pc.principal(CONTRACT_ADDR).willSendLte(0).ustx();

  for (let i = 0; i < appBtcAddrs.length; i++) {
    const [appBtcAddr, pred] = [appBtcAddrs[i], preds[i]];
    const { contract, stxAddr, seq, targetBurnHeight } = pred;

    const targetHeight = await dataApi.fetchHeight(targetBurnHeight);

    let functionName = 'verify';
    let functionArgs = [Cl.principal(stxAddr), Cl.uint(seq), Cl.uint(targetHeight)];
    if (targetHeight === -1) {
      functionName = 'not-available';
      functionArgs = [Cl.principal(stxAddr), Cl.uint(seq)];
    }

    const txOptions = {
      network: 'mainnet',
      senderKey: SENDER_KEY,
      contractAddress: CONTRACT_ADDR,
      contractName: contract,
      functionName,
      functionArgs,
      postConditionMode: PostConditionMode.Deny,
      postConditions: [condition1, condition2],
      //fee: 100,
      validateWithAbi: true,
    };
    /** @ts-expect-error */
    const transaction = await makeContractCall(txOptions);
    const response = await broadcastTransaction({ transaction, network: 'mainnet' });
    console.log(`(${logKey}) ${pred.id} called the contract`);

    const newPred = mergePreds(pred, { vTxId: response.txid, targetHeight });
    await dataApi.updatePred(appBtcAddr, newPred);
    console.log(`(${logKey}) ${pred.id} saved to Datastore`);
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
