import { Datastore } from '@google-cloud/datastore';

import { PLYR_LOG } from './const';
import { isObject } from './utils';

const datastore = new Datastore();

const getLastestPlyrLog = async () => {
  const query = datastore.createQuery(PLYR_LOG);
  query.order('updateDate', { descending: true });
  query.limit(1);

  const [entities] = await datastore.runQuery(query);

  let log = null;
  if (Array.isArray(entities)) {
    for (const entity of entities) {
      if (!isObject(entity)) continue;

      log = entityToPlyrLog(entity);
    }
  }
  return log;
};

const savePlyrLog = async (log) => {
  const entity = { key: datastore.key([PLYR_LOG]), data: plyrLogToEntityData(log) };
  await datastore.save(entity);
};

const plyrLogToEntityData = (log) => {
  const uKeys = log.uKeys.join(',');
  const pKeys = log.pKeys.join(',');

  const data = [
    { name: 'uUdtDt', value: new Date(log.uUdtDt) },
    { name: 'uKeys', value: uKeys, excludeFromIndexes: true },
    { name: 'pUdtDt', value: new Date(log.pUdtDt) },
    { name: 'pKeys', value: pKeys, excludeFromIndexes: true },
    { name: 'createDate', value: new Date(log.createDate) },
    { name: 'updateDate', value: new Date(log.updateDate) },
  ];
  return data;
};

const entityToPlyrLog = (entity) => {
  const log = {
    uUdtDt: entity.uUdtDt.getTime(),
    uKeys: entity.uKeys.split(','),
    pUdtDt: entity.pUdtDt.getTime(),
    pKeys: entity.pKeys.split(','),
    createDate: entity.createDate.getTime(),
    updateDate: entity.updateDate.getTime(),
  };
  return log;
};

const data = { getLastestPlyrLog, savePlyrLog };

export default data;
