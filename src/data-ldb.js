import { Datastore } from '@google-cloud/datastore';
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

  return user;
};

const data = { getUsers, fetchStorage, updateStorage };

export default data;
