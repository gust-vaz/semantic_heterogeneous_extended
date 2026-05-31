// Waits for all three members to be reachable, then initiates the replica set.
// Run by the mongo-init one-shot container.
var maxAttempts = 60;
var attempt = 0;
var initiated = false;

while (attempt < maxAttempts && !initiated) {
  try {
    rs.initiate({
      _id: "rs0",
      members: [
        { _id: 0, host: "mongo-primary:27017", priority: 2 },
        { _id: 1, host: "mongo-secondary-1:27018", priority: 1 },
        { _id: 2, host: "mongo-secondary-2:27019", priority: 1 }
      ]
    });
    initiated = true;
    print("Replica set initiated successfully.");
  } catch (e) {
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
