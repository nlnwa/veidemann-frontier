Seeds:

- aaa
- bbb
- bbb/p
- tu
- faktisk

|                                              |      url | parent seed | seed i kø | beste seed match | Resultat      |
| -------------------------------------------- | -------: | ----------- | ---------- | ---------------- | ------------- |
| 1. gangshøsting av seed                     |      aaa | aaa         |            | aaa              | aaa           |
| n'te gangs høsting av seed                  |      aaa | aaa         | aaa        | aaa              | aaa           |
| 1. gangshøsting utlenke til annen seed      |      bbb | aaa         |            | bbb              | bbb           |
| 1. gangshøsting utlenke til annen seed      |      ccc | aaa         |            |                  | aaa           |
| n'te gangshøsting av utlenke til annen seed |      bbb | aaa         | bbb        | bbb              | bbb           |
| n'te gangshøsting av utlenke til annen seed |      ccc | aaa         | aaa        |                  | aaa           |
| 1. gangshøsting utlenke til annen seed      |    bbb/p | bbb         |            | bbb/p            | bbb/p         |
| n'te gangshøsting utlenke til annen seed    |    bbb/p | bbb         | bbb/p      | bbb/p            | bbb/p         |
| n'te gangshøsting utlenke til annen seed    |   tu/art | faktisk     |            | tu               | faktisk OR tu |
| n'te gangshøsting utlenke til annen seed    | .com/art | faktisk     |            |                  | faktisk       |

# Examples
Following examples are based on harvesting the following structure. Seed is `faktisk.no`.

```mermaid
flowchart LR
    e1[fa:fa-circle-1 faktisk.no] --> e2[fa:fa-2 faktisk.no/artikkel] --> e3[vg.no/artikkel]
    e2 --> e4[cnn.com/article]
```

## Example 1

### Seeds:

| seed       | scope rules                                                |
|------------|------------------------------------------------------------|
| faktisk.no | scope accepts everything in domain plus one off-domain hop |
| vg.no      | scope accepts everything in domain, but no off-domain hops |

### First harvest of domain `faktisk.no`

```mermaid
stateDiagram
    direction LR

    e1: faktisk.no
    note left of e1
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e2: faktisk.no/artikkel
    note left of e2
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e3: vg.no/artikkel
    note right of e3
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: vg.no
        Evaluation in context of: faktisk.no
    end note
  
    e4: cnn.com/article
    note right of e4
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note

    e1 --> e2
    e2 --> e3
    e2 --> e4
```

### Second harvest of domain `faktisk.no`

```mermaid
stateDiagram
    direction LR

    e1: faktisk.no
    note left of e1
        Parent seed: faktisk.no
        Seed in queue: faktisk.no
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e2: faktisk.no/artikkel
    note left of e2
        Parent seed: faktisk.no
        Seed in queue: faktisk.no
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e3: vg.no/artikkel
    note right of e3
        Parent seed: faktisk.no
        Seed in queue: faktisk.no
        Best seed match: vg.no
        Evaluation in context of: faktisk.no
    end note
  
    e4: cnn.com/article
    note right of e4
        Parent seed: faktisk.no
        Seed in queue: faktisk.no
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note

    e1 --> e2
    e2 --> e3
    e2 --> e4
```

## Example 2

### Seeds:

| seed       | scope rules                                                |
|------------|------------------------------------------------------------|
| faktisk.no | scope accepts everything in domain, but no off-domain hops |
| vg.no      | scope accepts everything in domain, but no off-domain hops |

### First harvest of domain `faktisk.no`

```mermaid
stateDiagram
    direction LR

    e1: faktisk.no
    note left of e1
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e2: faktisk.no/artikkel
    note left of e2
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e3: vg.no/artikkel
    note right of e3
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: vg.no
        Evaluation in context of: vg.no
    end note
  
    e4: cnn.com/article
    note right of e4
        Parent seed: faktisk.no
        Seed in queue:
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note

    e1 --> e2
    e2 --> e3
    e2 --> e4
```

### Second harvest of domain `faktisk.no`

```mermaid
stateDiagram
    direction LR

    e1: faktisk.no
    note left of e1
        Parent seed: faktisk.no
        Seed in queue: faktisk.no
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e2: faktisk.no/artikkel
    note left of e2
        Parent seed: faktisk.no
        Seed in queue: faktisk.no
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note
  
    e3: vg.no/artikkel
    note right of e3
        Parent seed: faktisk.no
        Seed in queue: vg.no
        Best seed match: vg.no
        Evaluation in context of: vg.no
    end note
  
    e4: cnn.com/article
    note right of e4
        Parent seed: faktisk.no
        Seed in queue: faktisk.no
        Best seed match: faktisk.no
        Evaluation in context of: faktisk.no
    end note

    e1 --> e2
    e2 --> e3
    e2 --> e4
```
