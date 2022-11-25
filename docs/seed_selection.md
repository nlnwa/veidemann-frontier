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

```mermaid
flowchart TB
    classDef comment fill:#efa,color:#000;

    e1[faktisk.no] --> e2[faktisk.no/artikkel] --> e3[vg.no/artikkel]
    e2 --> e4[cnn.com/article]
    
    subgraph seed [ ]
        direction TB
        k1[<b>Seed</b>\n scope evaluation\n in context of seed]:::comment --- e1
    end
    
    subgraph link [ ]
        direction TB
        k2[<b>Link on seeds domain</b>\nScope evaluation in context of seed]:::comment --- e2
    end
    
    subgraph offsite1 [ ]
        direction TB
        e3 --- k3[<b>Link on domain which has own seed</b>\nScope evaluation in context of original seed.\n If not included, scope is evaluated in\n context of links own seed]:::comment
    end
    
    subgraph offsite2 [ ]
        direction TB
        e4 --- k4[<b>Link on domain with no own seed</b>\nScope evaluation in context of seed]:::comment
    end
```

```mermaid
flowchart LR
    e1[fa:fa-circle-1 faktisk.no] --> e2[fa:fa-2 faktisk.no/artikkel] --> e3[vg.no/artikkel]
    e2 --> e4[cnn.com/article]
```
