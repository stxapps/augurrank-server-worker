import {
  fetchNonce, makeContractCall, broadcastTransaction, PostConditionMode, Cl,
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
  const { preds } = await dataApi.getVerifiablePreds(burnHeight);
  console.log(`(${logKey}) got ${preds.length} verifiable preds`);

  let nonce = await fetchNonce({ address: CONTRACT_ADDR });

  for (const pred of preds) {
    const { stxAddr, contract, seq, targetBurnHeight } = pred;

    const targetHeight = await dataApi.fetchHeight(targetBurnHeight);
    console.log(`(${logKey}) pred: ${pred.id} got targetHeight: ${targetHeight}`);

    let functionName = 'verify';
    let functionArgs = [Cl.principal(stxAddr), Cl.uint(seq)];
    let fee = 4191; // might diff btw. verify and not-available
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
      fee,
      nonce,
      validateWithAbi: true,
    };
    /** @ts-expect-error */
    const transaction = await makeContractCall(txOptions);
    const response = await broadcastTransaction({ transaction, network: 'mainnet' });
    console.log(`(${logKey}) called the contract with txid: ${response.txid}`);

    let txId = response.txid;
    if (!txId.startsWith('0x')) txId = '0x' + txId;

    const newPred = mergePreds(pred, { vTxId: txId, targetHeight });
    await dataApi.updatePred(newPred);
    console.log(`(${logKey}) saved newPred to Datastore`);

    nonce += BigInt(1);
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
