# Search Component
The main component that keeps track of the entries being added in the system. The component is at the heart of the application and is also responsible for informing other components about the change in the utility of the application.

## Protocols Running
The search component mainly encapsulates two different pull mechanisms.

### Control Pull Mechanism :
It is a generic pull mechanism which asks for an update from all the components in the system, collates it and sends it to the requesting node, which the node decodes and updates it own internal state. It is a way to disseminate important updates in the system.

### Entry Pull Mechanism:
It's similar to the control pull mechanism but the frequency of pull is higher than the control pull mechanism as the node wants to quickly catch up to the nodes in the system that are higher than him.


## Invariants:
* A node always pulls based on the epoch update that it receives through the control pull mechanism.
* The node asks for the next epoch based on the highest epoch it has pulled. It is helpful in `taking sides` in case of a network partitioning.
* The epoch update pull always happen `in-order` and I should never pull out of order except during the case of `partitioning merge`.


## Epoch Update Addition Mechanism:
* The epoch update objects are created by the nodes themselves when the leader publishes a landing entry to the leader group nodes in the system.

* When a new leader gets elected, it needs to close the epoch of the previous leader. This can be tricky. In very `rare case` if majority of the nodes die, then there might be a case in which the nodes that are way below in terms of utility might suddenly find themselves in situation of being adequate for becoming leader but might have a lot of index entries pending. So what should the next epoch be ?

* Should the epoch number be the next to what you are trying to download or the next to the epoch updates that you already have downloaded ?
