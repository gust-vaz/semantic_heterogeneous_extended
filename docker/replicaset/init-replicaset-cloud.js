// Initiates the rs0 replica set across its member VMs. Run by init-replicaset.sh
// in a `mongosh --nodb` container on the control VM. REPLSET_HOSTS carries the
// members' routable private IPs (ordered, primary first); the first member is
// pinned PRIMARY by a higher priority, so run.sh can label the rest secondaries
// without probing. A bounded serverSelectionTimeoutMS makes a wrong address fail
// fast. Re-running must be harmless, so an already-initialised set is left alone.
//
// Cloud-only: there is deliberately no Compose-name fallback (the local stacks
// use docker/replicaset/init-replicaset*.js). This file connects explicitly with
// new Mongo(), the init-sharded.js style, not the implicit `rs` helper.

const ALREADY_INITIALIZED = 23;
const NOT_YET_INITIALIZED = 94;
const CONNECT_ATTEMPTS = parseInt(process.env.REPLSET_CONNECT_ATTEMPTS || "60", 10);

const HOSTS = (process.env.REPLSET_HOSTS || "")
  .split(",").map(function (s) { return s.trim(); }).filter(Boolean);

if (HOSTS.length === 0) {
  print("ERROR: REPLSET_HOSTS is required (comma-separated host:port, primary first).");
  quit(1);
}

// The first member leads: a higher priority makes it the deterministic PRIMARY.
const members = HOSTS.map(function (host, i) {
  return { _id: i, host: host, priority: i === 0 ? 2 : 1 };
});
const config = { _id: "rs0", members: members };
print("Initiating rs0 with members: " + JSON.stringify(config));

function connect(address) {
  for (let attempt = 0; attempt < CONNECT_ATTEMPTS; attempt++) {
    try {
      const admin = new Mongo(
        "mongodb://" + address + "/?serverSelectionTimeoutMS=2000").getDB("admin");
      admin.adminCommand({ ping: 1 });
      return admin;
    } catch (e) {
      sleep(1000);
    }
  }
  throw new Error("could not reach " + address);
}

try {
  const admin = connect(HOSTS[0]);
  try {
    admin.adminCommand({ replSetGetStatus: 1 });
    print("rs0: already initialized.");
  } catch (e) {
    if (e.code !== NOT_YET_INITIALIZED) throw e;
    try {
      admin.adminCommand({ replSetInitiate: config });
      print("rs0: initiated.");
    } catch (e2) {
      if (e2.code !== ALREADY_INITIALIZED) throw e2;
      print("rs0: already initialized.");
    }
  }
  // Wait for a PRIMARY so run.sh's first write lands on member1.
  for (let attempt = 0; attempt < 60; attempt++) {
    if (connect(HOSTS[0]).adminCommand({ hello: 1 }).isWritablePrimary) {
      print("rs0 is healthy with a PRIMARY elected.");
      quit(0);
    }
    sleep(1000);
  }
  throw new Error("no primary after waiting");
} catch (e) {
  print("ERROR: " + e.message);
  quit(1);
}
