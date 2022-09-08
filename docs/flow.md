# Varianter

|    | 1. harvest | response | TS < queue | oos     |  robots deny | dns fail | 
|----|------------|----------|------------|---------|--------------|----------|
|  1 | &check;    | &check;  |            |         |              |          |
|  2 | &check;    | &check;  |            | &check; |              |          |
|  3 | &check;    | &check;  |            |         | &check;      |          |
|  4 | &check;    |          |            |         |              |          |
|  5 | &check;    |          |            |         |              | &check;  |
|  6 |            | &check;  | &check;    |         |              |          |
|  7 |            | &check;  | &check;    | &check; |              |          |
|  8 |            | &check;  | &check;    |         | &check;      |          |
|  9 |            | &check;  | &check;    |         |              | &check;  |
| 10 |            | &check;  |            |         |              |          |
| 11 |            | &check;  |            | &check; |              |          |
| 12 |            | &check;  |            |         | &check;      |          |
| 13 |            |          |            |         |              |          |
| 14 |            |          |            |         |              | &check;  |

### Hovedrutine

```mermaid
flowchart TD
    classDef in fill:#5d7;
    classDef out fill:#ff5;
    classDef proc fill:#efa;

    s1[Reserver neste\n ledige HOST] --> s2[Hent første\n URI fra HOST] --> s3[[Evaluering av URL]]:::proc
    s3 -->|Skal ikke høstes| s2
    s3 -->|Skal høstes| s6[Høst URL]
    s6 --> s7[Beregn neste\n TS for URL] --> s8[Oppdater kø] --> s9[Frigjør HOST]
    s6 --> l1{For alle lenker\n i dokumentet} --> l2[[Evaluering av URL]]:::proc
    l2 -->|Skal høstes| l3[Beregn neste\n TS for URL] --> l4[Oppdater kø] --> l1
    l2 -->|Skal ikke høstes| l1
```

### Prosedyre for evaluering av URL

```mermaid
flowchart TD
    classDef in fill:#5d7;
    classDef out fill:#ff5;
    classDef proc fill:#efa;

    e1([INN: URL]):::in --> e2[Gjør DNS oppslag] --> e3[Gjør robots.txt oppslag] --> e4{Skal URL\n høstes?}
    e4 -->|Ja| e5([UT: Skal høstes]):::out
    e4 -->|Nei| e6{Skal URL\n Svartelistes?}
    e6 -->|Ja| e7[Fjern URL fra kø] --> e8[Legg URL i svarteliste] --> e9([UT: Skal ikke høstes]):::out
    e6 -->|Nei| e9
```


## Host

- **Key:** Busy TS HOST
- **Value:** Response time


## URL queue

- **Key:** HOST TS COLLECTION URL
- **Val:** LastFetch ErrorCount NotFoundCount


## Cookie jar

- **Key:** COLLECTION HOST
- **Val:** Cookies


# Utfordringer

* [Host reservation service](host_reservation_service.md)
* Evaluering av config (scope)
* Statistikk
    * I hvilken grad overholdes høstefrekvens-kontrakt
    * Rapporter basert på domene og tid
* Høst en gang URLer
* Avslutte høsting av URL (Svartelisting)
* Høste samme seed/URL med forskjellige cookies


# Roadmap

* Bare hostbasert politeness
* Fjerne collections


# Collections

Hvis vi skal beholde collections, så bør disse også påvirke selve innhøstingen og ikke bare være en måte å navngi
WARC-filer på som i dag. Et forslag er å inkludere collection som en del av URL. Hvis samme seed puttes i flere
collections vil URLene høstes flere ganger helt uavhengig av hverandre (politeness blir likevel respektert). Denne
løsningen kan brukes for å høste samme nettsted med og uten pålogging eller med forskjellige browserinnstillinger.
