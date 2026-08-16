// Waits for all five members to be reachable, then initiates the replica set.
// Run by the mongo-init one-shot container.
var maxAttempts = 60;
var attempt = 0;

// Re-running this script must be harmless: `runner` depends on mongo-init
// completing successfully, so failing here would break every
// `docker compose run runner` against an already-initialised replica set.
function isInitiated() {
  try {
    rs.status();
    return true;   // rs.status() only succeeds once the set is initiated
  } catch (e) {
    return false;  // NotYetInitialized (code 94), or not reachable yet
  }
}

var initiated = isInitiated();
if (initiated) {
  print("Replica set already initialized. Skipping rs.initiate().");
}

while (attempt < maxAttempts && !initiated) {
  try {
    rs.initiate({
      _id: "rs0",
      members: [
        { _id: 0, host: "mongo-primary:27017", priority: 2 },
        { _id: 1, host: "mongo-secondary-1:27018", priority: 1 },
        { _id: 2, host: "mongo-secondary-2:27019", priority: 1 },
        { _id: 3, host: "mongo-secondary-3:27020", priority: 1 },
        { _id: 4, host: "mongo-secondary-4:27021", priority: 1 }
      ]
    });
    initiated = true;
    print("Replica set initiated successfully.");
  } catch (e) {
    if (String(e.message).indexOf("already initialized") !== -1) {
      print("Replica set already initialized. Continuing.");
      initiated = true;
      break;
    }
    print("Attempt " + attempt + " failed: " + e.message + ". Retrying...");
    sleep(2000);
    attempt++;
  }
}

if (!initiated) {
  print("ERROR: Could not initiate replica set after " + maxAttempts + " attempts.");
  quit(1);
}

// Wait for PRIMARY to be elected
var electionAttempts = 30;
attempt = 0;
var isPrimary = false;

while (attempt < electionAttempts && !isPrimary) {
  sleep(2000);
  try {
    var status = rs.status();
    for (var i = 0; i < status.members.length; i++) {
      if (status.members[i].stateStr === "PRIMARY") {
        isPrimary = true;
        break;
      }
    }
  } catch (e) {}
  attempt++;
}

if (!isPrimary) {
  print("ERROR: No PRIMARY elected after waiting.");
  quit(1);
}

print("Replica set is healthy with a PRIMARY elected.");
