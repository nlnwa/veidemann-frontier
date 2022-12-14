# Algorithm for connecting URL to seed

## Definitions

&#x24c5; Parent URL's seed\
&#x24c8; Seed stored in queued URL\
&#x24b7; Best seed match for URL

## Rules

```mermaid
flowchart TD
    s([Start]) --> n1{ \u24c5 = nil? }-->|Yes| n3[URL is seed\n Set \u24c5 and \u24c8 to URL] -->n2
    n1 -->|No| n2{ \u24b7 = nil?} -->|No| n4{ \u24c5 = \u24b7? } -->|Yes| n5[Scope context is \u24c5\n \u24c8 is set to \u24c5] --> e
    n2 -->|Yes| n6[URL has no matching seed\nScope context is \u24c5]
    n4 -->|No| n9{ URL is in\n \u24c5's scope? } -->|Yes| n10[ \u24c8 is set to \u24c5 ] -->e
    n9-->|No| n11[ \u24c8 is set to \u24b7 ] -->e
    n6 --> n7{ \u24c8 = nil? } -->|Yes| n8[ \u24c8 is set to \u24c5] --> e
    n7 -->|No| e
    e([End])
```


# Examples

Following examples are based on harvesting the following structure. Seeds are `faktisk.no` and `vg.no`.

```mermaid
stateDiagram
    direction LR
    e11: faktisk.no
    e12: faktisk.no/artikkel
    e13: vg.no/artikkel
    e14: vg.no/artikkel2
    e15: cnn.com/article
    e16: vg.no
    e21: vg.no
    e22: vg.no/artikkel
    e23: vg.no/artikkel2
    
    e11 --> e12
    e12 --> e13
    e13 --> e14
    e12 --> e15
    e12 --> e16
    
    e21 --> e22
    e22 --> e23
```

## Example 1

### Seeds:

| seed       | scope rules                                                |
|------------|------------------------------------------------------------|
| faktisk.no | scope accepts everything in domain plus one off-domain hop |
| vg.no      | scope accepts everything in domain, but no off-domain hops |

### Legend

&#x24c5; Parent seed\
&#x24c8; Stored in queue\
&#x24b7; Best seed match

### First harvest of domain `faktisk.no`

```mermaid
stateDiagram
    direction LR 

    e11: faktisk.no
    e11: \u24c5 faktisk.no
    e11: \u24c8
    e11: \u24b7 faktisk.no
    note left of e11
        Evaluation in context of: \u24c5 faktisk.no
    end note
  
    e12: faktisk.no/artikkel
    e12: \u24c5 faktisk.no
    e12: \u24c8
    e12: \u24b7 faktisk.no
    note left of e12
        Evaluation in context of: faktisk.no
    end note
  
    e13: vg.no/artikkel
    e13: \u24c5 faktisk.no
    e13: \u24c8
    e13: \u24b7 vg.no
    note right of e13
        Evaluation in context of: faktisk.no
    end note
  
    e14: vg.no/artikkel2
    e14: \u24c5 faktisk.no
    e14: \u24c8
    e14: \u24b7 vg.no
    note right of e14
        Evaluation in context of: vg.no
    end note

    e15: cnn.com/article
    e15: \u24c5 faktisk.no
    e15: \u24c8
    e15: \u24b7 faktisk.no
    note right of e15
        Evaluation in context of: faktisk.no
    end note

    e16: vg.no
    e16: \u24c5 faktisk.no
    e16: \u24c8
    e16: \u24b7 vg.no
    note right of e16
        Evaluation in context of: faktisk.no
    end note

    e21: vg.no
    e21: \u24c5 vg.no
    e21: \u24c8
    e21: \u24b7 vg.no
    note left of e21
        Evaluation in context of: \u24c5: vg.no
    end note

    e22: vg.no/artikkel
    e22: \u24c5 vg.no
    e22: \u24c8 faktisk.no
    e22: \u24b7 vg.no
    note left of e22
        Evaluation in context of: \u24c5: vg.no
        Set \u24c8 to vg.no since \u24c5 = \u24b7 = vg.no
    end note

    e23: vg.no/artikkel2
    e23: \u24c5 vg.no
    e23: \u24c8 vg.no
    e23: \u24b7 vg.no
    note right of e23
        Evaluation in context of: \u24c5: faktisk.no
    end note
   
    e11 --> e12
    e12 --> e13
    e13 --> e14
    e12 --> e15
    e12 --> e16
    
    e21 --> e22
    e22 --> e23
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
