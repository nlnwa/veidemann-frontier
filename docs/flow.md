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

```mermaid
flowchart TD
    classDef in fill:#5d7;
    classDef out fill:#ff5;
    classDef proc fill:#efa;

    s1[Hent neste\n ledige CHG] --> s2[Hent første\n URI fra CHG] --> s3[[Evaluering av URL]]:::proc
    s3 -->|Skal ikke høstes| s2
    s3 -->|Skal høstes| s6[Høst URL]
    s6 --> s7[Sjekk når URL skal\n tilbake i køen] --> s8[Legg tilbake i kø] --> s9[Beregn neste TS for CHG] --> s10[Legg CHG tilbake i kø]
    s6 --> l1{For alle lenker\n i dokumentet} --> l2[[Evaluering av URL]]:::proc
    l2 -->|Skal høstes| l3[Sjekk når URL\n tidligst kan høstes] --> l4[Legg i kø] --> l1
    l2 -->|Skal ikke høstes| l1
```

Prosedyre for evaluering av URL

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

## CHG

- **Key:** Busy TS ID
- **Value:**

## Host

- **Key:** Busy TS ID
- **Value:** Response time

## URL queue

- **Key:** CHG TS URL
- **Val:** LastFetch Error Cookies?

## Cookie jar

- **Key:** HOST
- **Val:** Cookies

# Utfordringer

* Host reservasjon service
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