import {
  makeContractCall, broadcastTransaction, PostConditionMode, Cl,
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

  for (let i = 0; i < appBtcAddrs.length; i++) {
    const [appBtcAddr, pred] = [appBtcAddrs[i], preds[i]];
    const { contract, stxAddr, seq, targetBurnHeight } = pred;

    const targetHeight = await dataApi.fetchHeight(targetBurnHeight);
    console.log(`(${logKey}) pred: ${pred.id} got targetHeight: ${targetHeight}`);

    let functionName = 'verify';
    let functionArgs = [Cl.principal(stxAddr), Cl.uint(seq)];
    if (targetHeight === -1) {
      functionName = 'not-available';
    } else {
      functionArgs.push(Cl.uint(targetHeight));
    }

    const txOptions = {
      network: 'mainnet',
      senderKey: SENDER_KEY,
      contractAddress: CONTRACT_ADDR,
      contractName: contract,
      functionName,
      functionArgs,
      postConditionMode: PostConditionMode.Deny,
      postConditions: [],
      fee: 4191, //22000 // might diff btw. verify and not-available
      //nonce: n, // can set at the top and plus one, no need to fetch every time
      validateWithAbi: true,
    };
    /** @ts-expect-error */
    const transaction = await makeContractCall(txOptions);
    const response = await broadcastTransaction({ transaction, network: 'mainnet' });
    console.log(`(${logKey}) called the contract with txid: ${response.txid}`);

    const newPred = mergePreds(pred, { vTxId: response.txid, targetHeight });
    await dataApi.updatePred(appBtcAddr, newPred);
    console.log(`(${logKey}) saved newPred to Datastore`);

    // need to wait to be confirmed? can do next one immediately?
    break;
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
