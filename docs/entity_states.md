# Entity States
Links contain and operate on entities. A specific entity is unique within a link and can be part of one or more components (source, outbound, local).

## States
Each entity is in one of the following states at any given time:
* Idle: This is the default state that entities start in.
* Activated: The entity is in the process of being pulled/deleted to/from the local side. It is only present in the source side of the link.
* Received: The entity is in the process of being pulled/deleted to/from the local side. It is present in both sides of the link.
* Pulled: The entity has been copied from the source to the local side.
* Tainted: The entity was flagged by the source side indicating to the local side to delete it.
* Deprecated: The entity was flagged by the source side and subsequently deleted by the local side.

## Transitions
The following state diagram shows the different states that entities can be in and how they can transition between these states:

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Activated: pulled / add to outbound, start pull process
    Activated --> Received: processed [in pull process] / add to local
    Received --> Pulled: processed [in pull process] / finish pull process
    Pulled --> Received: deleted / start delete process
    Received --> Activated: processed [in delete process] / remove from local
    Activated --> Idle: processed [in delete process and not flagged] / remove from outbound, finish delete process
    Pulled --> Tainted: flagged / flag
    Tainted --> Pulled: unflagged / unflag
    Tainted --> Received: deleted / start delete process
    Activated --> Deprecated: processed [in delete process and flagged] / remove from outbound, finish delete process
    Deprecated --> Idle: unflagged / unflag
```

The diagram adheres to the following rule to avoid entities with invalid states due to interruptions (e.g. connection losses):

**Never modify both sides of the link during a single transition.**

Not following this rule can lead to entities in invalid states due to modifying one side of the link and then losing connection.

## Persistence
Storing an entity's state directly in the persistent layer is problematic because it makes it difficult to have state transitions that only modify one side of the link. An easier approach is to map the state an entity has in the domain model to the state it has in the persistent layer. This persistent state consists of the entity's presence in the three components and whether it has an process and/or flag or not.

The following table illustrates the chosen mapping:

| In source | In outbound | In local | Has process | Is flagged | State |
|--------|----------|-------|--------|---------|---------|
| :white_check_mark: | :x: | :x: | :x: | :x: | Idle |
| :white_check_mark: | :white_check_mark: | :x: | :white_check_mark: | :x: | Activated |
| :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x: | Received|
| :white_check_mark: | :white_check_mark: | :white_check_mark: | :x: | :x: | Pulled |
| :white_check_mark: | :white_check_mark: | :white_check_mark: | :x: | :white_check_mark: | Tainted |
| :white_check_mark: | :x: | :x: | :x: | :white_check_mark: | Deprecated |

## Processes
Idle entities can be pulled from the source side into the local side and once they are pulled they can be deleted from the local side. Activated and received entities are currently undergoing one of these two processes. The name of the specific process is associated with entities that are in the aforementioned states. This allows us to correctly transition these entities. For example without associating the process with the entity we would not be able to determine whether an activated entity should become a received one (pull) or an idle one (delete).
