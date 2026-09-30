// Initiates the config-server and shard replica sets, then registers every shard
// with the router. Run by the mongo-init one-shot container as `mongosh --nodb`,
// with SHARD_COUNT set by the compose file. Every connection is opened here, with
// retries, so the script never races the router's start-up.
//
// Re-running this must be harmless: `runner` depends on mongo-init completing
// successfully, so failing here would break every run against a cluster that is
// already up. Every step below is safe to repeat.
//
// mongosh throws on a failed command rather than returning {ok: 0}, so each step
// decides by the error code it catches, never by inspecting a returned document.

const SHARD_COUNT = parseInt(process.env.SHARD_COUNT || "4", 10);
const NOT_YET_INITIALIZED = 94;
const ALREADY_INITIALIZED = 23;

// Addresses default to the Compose service names so a local stack is unchanged;
// a multi-VM cloud cluster passes routable private IPs through the environment.
const CONFIG_HOST = process.env.CONFIG_HOST || "cfg1:27019";
const ROUTER_HOST = process.env.ROUTER_HOST || "mongos:27017";
const SHARD_HOSTS = (process.env.SHARD_HOSTS || "")
  .split(",").map(function (s) { return s.trim(); }).filter(Boolean);

// Fail before any connection: a short list would register the wrong shards.
if (SHARD_HOSTS.length && SHARD_HOSTS.length !== SHARD_COUNT) {
  print("ERROR: SHARD_HOSTS has " + SHARD_HOSTS.length +
        " entries but SHARD_COUNT is " + SHARD_COUNT);
  quit(1);
}

function shardAddress(i) {   // i is 1-based
  return SHARD_HOSTS.length ? SHARD_HOSTS[i - 1] : ("shard" + i + ":27018");
}

function connect(address) {
  for (let attempt = 0; attempt < 60; attempt++) {
    try {
      const admin = new Mongo(address).getDB("admin");
      admin.adminCommand({ ping: 1 });
      return admin;
    } catch (e) {
      sleep(1000);
    }
  }
  throw new Error("could not reach " + address);
}

function initiate(address, setName, isConfigServer) {
  const admin = connect(address);
  try {
    admin.adminCommand({ replSetGetStatus: 1 });
    print(setName + ": already initialized.");
    return;
  } catch (e) {
    if (e.code !== NOT_YET_INITIALIZED) throw e;
  }

  const config = { _id: setName, members: [{ _id: 0, host: address }] };
  if (isConfigServer) config.configsvr = true;
  try {
    admin.adminCommand({ replSetInitiate: config });
    print(setName + ": initiated.");
  } catch (e) {
    if (e.code !== ALREADY_INITIALIZED) throw e;
    print(setName + ": already initialized.");
  }
}

function waitForPrimary(address, setName) {
  const admin = connect(address);
  for (let attempt = 0; attempt < 60; attempt++) {
    if (admin.adminCommand({ hello: 1 }).isWritablePrimary) return;
    sleep(1000);
  }
  throw new Error(setName + ": no primary after 60s");
}

function addShard(router, spec) {
  // Registering a shard that is already registered with the same host succeeds,
  // so this is idempotent as it stands. The retries only cover a router that has
  // not yet reached the config server.
  let lastError = null;
  for (let attempt = 0; attempt < 30; attempt++) {
    try {
      router.adminCommand({ addShard: spec });
      print("shard registered: " + spec);
      return;
    } catch (e) {
      lastError = e;
      sleep(2000);
    }
  }
  throw lastError;
}

try {
  const sets = [{ address: CONFIG_HOST, name: "cfg", configServer: true }];
  for (let i = 1; i <= SHARD_COUNT; i++) {
    sets.push({ address: shardAddress(i), name: "shard" + i, configServer: false });
  }
  for (const set of sets) initiate(set.address, set.name, set.configServer);
  for (const set of sets) waitForPrimary(set.address, set.name);

  const router = connect(ROUTER_HOST);
  for (let i = 1; i <= SHARD_COUNT; i++) {
    addShard(router, "shard" + i + "/" + shardAddress(i));
  }

  const registered = router.getSiblingDB("config").shards.countDocuments({});
  if (registered !== SHARD_COUNT) {
    throw new Error("expected " + SHARD_COUNT + " shards, found " + registered);
  }
  print("Sharded cluster ready with " + registered + " shards.");
} catch (e) {
  print("ERROR: " + e.message);
  quit(1);
}
