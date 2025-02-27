import { Datastore, PropertyFilter } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import { USER } from './const';
import { isObject, isNotNullIn } from './utils';

const datastore = new Datastore();
const storage = new Storage();

const getUsers = async (stxAddrs) => {
  const keys = stxAddrs.map(stxAddr => datastore.key([USER, stxAddr]));
  const [entities] = await datastore.get(keys);

  const users = [];
  if (Array.isArray(entities)) {
    for (const entity of entities) {
      if (!isObject(entity)) continue;

      const user = entityToUser(entity);
      users.push(user);
    }
  }

  return { users };
};

const getUpdatedUsers = async (updateDate) => {
  const query = datastore.createQuery(USER);
  query.filter(new PropertyFilter('updateDate', '>=', new Date(updateDate)));
  query.order('updateDate', { descending: false });
  query.limit(100);

  const [entities] = await datastore.runQuery(query);

  const users = [];
  if (Array.isArray(entities)) {
    for (const entity of entities) {
      if (!isObject(entity)) continue;

      const user = entityToUser(entity);
      users.push(user);
    }
  }

  return { users };
};

const fetchStorage = async (path) => {
  const file = storage.bucket('augurrank-001.appspot.com').file(path);

  const [doExist] = await file.exists();
  if (!doExist) return null;

  const [res] = await file.download();
  const obj = JSON.parse(res.toString());
  return obj;
};

const updateStorage = async (path, content, cacheControl) => {
  const file = storage.bucket('augurrank-001.appspot.com').file(path);
  const opts = {
    public: true, metadata: { contentType: 'application/json', cacheControl }
  }
  await file.save(JSON.stringify(content), opts);
};

const deleteStorage = async (dir) => {
  const query = { prefix: dir };
  const [files] = await storage.bucket('augurrank-001.appspot.com').getFiles(query);

  const nFiles = 64;
  for (let i = 0; i < files.length; i += nFiles) {
    const sltdFiles = files.slice(i, i + nFiles);
    await Promise.all(sltdFiles.map(file => file.delete()));
  }
};

const entityToUser = (entity) => {
  const user = {
    stxAddr: entity[datastore.KEY].name,
    createDate: entity.createDate.getTime(),
    updateDate: entity.updateDate.getTime(),
  };
  if (isNotNullIn(entity, 'username')) {
    user.username = entity.username;
    user.usnVrfDt = null;
    if (isNotNullIn(entity, 'usnVrfDt')) user.usnVrfDt = entity.usnVrfDt.getTime();
  }
  if (isNotNullIn(entity, 'avatar')) {
    user.avatar = entity.avatar;
    user.avtVrfDt = null;
    if (isNotNullIn(entity, 'avtVrfDt')) user.avtVrfDt = entity.avtVrfDt.getTime();
  }
  if (isNotNullIn(entity, 'bio')) user.bio = entity.bio;
  if (isNotNullIn(entity, 'didAgreeTerms')) user.didAgreeTerms = entity.didAgreeTerms;
  if (isNotNullIn(entity, 'noInLdb')) user.noInLdb = entity.noInLdb;
  if (isNotNullIn(entity, 'noPlyrPg')) user.noPlyrPg = entity.noPlyrPg;

  return user;
};

const data = { getUsers, getUpdatedUsers, fetchStorage, updateStorage, deleteStorage };

export default data;
