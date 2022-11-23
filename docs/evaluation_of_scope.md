# Evaluation of scope

## Definisjoner

### Nivå vs. Dybde vs. Hopp fra seed

Analogt med dykking eller graving vil høsting av en seed starte på overflaten (dybde 0) og utlenker
høstes på neste nivå.

Hvis vi snakker om høsting på et eller annet gitt nivå er det ikke mulig å vite hva det betyr uten å definere startnivå.
F.eks. lider heis av dette problemet og det merkes derfor som regel hvilken knapp som er på overflaten.

1. Dybde 0 er det samme som 1. nivå er det samme som 0 hopp fra seed er det samme som seed URL
   er det samme som forsiden.
2. Dybde 1 er det samme som 2. nivå er det samme som 1 hopp fra seed er det samme som utlenker hos seed URL.
3. ...

### Seed deaktivert

Å deaktivere en SEED utsetter høsting av hele treet av URLer som er i skopet til SEED sine profiler.

### Profil deaktivert

Å deaktivere en PROFIL utsetter høsting av hele treet av URLer som er i skopet til en eller annen profil blant alle SEED
som er assosiert med PROFIL.

### Collection deaktivert

Å deaktivere en COLLECTION utsetter høsting av alle URLer som er assosiert med COLLECTION.

### Slette seed

Alle URLer i kø som er assosiert med SEED fjernes eller overstyres automatisk

### Slette profil

Bidrar ikke til utlenker. Kan føre til at URL som ikke lenger er i skopet blir fjernet eller overstyrt automatisk.

### Slette collection

Alle URLer i kø assosiert med COLLECTION blir fjernet fra kø automatisk.

### Prioritering mellom forskjellige collections på samme host

For å kunne prioritere en collection over en annen på samme host kan det tenkes at en collection
konfigureres med en vekt (et tall mellom 0 og 1) som representerer sjansen for at en URL
gir fra seg plassen i køen til en annen collection.

Det er ikke nødvendig å ta hensyn til vekt hvis det ikke er kniving om plassen i køen på et gitt tidspunkt.

### Prioritering mellom forskjellige URLer for samme collection på samme host med samme profil.

Det kan tenkes at det velges tilfeldig mellom et sett av URLer med samme collection på somme host som alle er
klare til å høstes på et gitt tidspunkt.

### Prioritering mellom forskjellige profiler

Det kan tenkes at to profiler som er konfigurert på samme seed "krangler" om å høste et sett av URLer i køen
på samme host.

Eksempel:

- En profil "forsider" er konfigurert til å høste med en frekvens på 1 gang per time til og med dybde 0.
- En annen profil "daglig" er konfigurert til å høste med en frekvens på 1 gang per dag til og med dybde 2.

Hvis det er mange URLer på dybde 1 og 2 i køen (pga. skopet til "daglig" profil) vil det potensielt få konsekvenser
for frekvensen til profilen "forsider".

Det kan tenkes at en profil konfigureres med en vekt som representerer sjansen for at en URL gir fra seg plassen i
køen til en annen profil.

La oss si at det på et gitt tidspunkt er 20 URLer med samme collection og på samme host som er klare til å høstes:

- 1 i skopet til profil "forsider"
- 19 i skopet til profil "daglig".

Hvis profil "daglig" er konfigurert med en vekt 0.9 vil det si at (i det lange løp) vil "daglig" profil
gi fra seg plassen i køen 18 av 20 ganger til profil "forsider".

## Hvordan starte høsting

Det finnes to tilnærminger på dette området. Den ene tilnærmingen er SEED/PROFIL/COLLECITON "startes" som beskrevet nedenfor eller at systemet automatisk
håndterer "oppstart" når en profil er tildelt en SEED eller går fra å være deaktivert til å bli aktivert.

### Starte seed

Å starte høsting av en seed er det samme som å legge til seedens URL som utlenke i kontekst
av én eller flere profiler.

### Starte profil

Å starte en profil starter alle seed som er assosiert med seg selv i kontekst av seg selv

### Starte collection

Å starte en collection starter alle seeds i kontekst av profil(er) som er assosiert med collection.

## Høste URL som ligger i kø

Gitt en URL i køen som har blitt valgt til å høstes og en kontekst med COLLECTION, URL, SEED, DEPTH, LENGTH,
TIME_TO_LIVE, LAST_FETCH_TIME
er følgende en algoritme for å behandle URL:

1. Hvis SEED ikke finnes må det tas stilling til om URL fjernes fra kø eller om URL er i skopet til en annen profil
og hva som eventuellt skal gjøres da.
2. Hvis COLLECTION ikke finnes fjernes URL fra kø.
3. Hvis SEED er deaktivert legges URL tilbake i kø med neste høstetidspunkt i fremtiden.
4. Hvis COLLECTION er deaktivert legges URL tilbake i kø med neste høstetidspunkt i fremtiden.
5. Hent alle profiler assosiert med SEED og som har COLLECTION
6. Sjekk om URL er i skopet (relativt til SEED) hos alle profiler fra forrige steg:

    1. Hvis URL ikke er i skopet fjernes URL fra kø.
    2. Hvis URL er i skopet til bare deaktiverte profiler legges URL tilbake i kø med høstetidspunkt i fremtiden.
    3. Hvis URL er i skopet må det kanskje gjøres en dobbelsjekk mot profilenes frekvens
       for å sikre at det er nødvendig å høste på nåvørende tidspunkt (f.eks. kan en profil ha fått langsommere frekvens
       siden URL ble lagt i kø).
7. Høst URL

## Prosessering av egen URL etter høsting

Egen URL behandles likt som en utlenke bortsett fra:

1. Sett siste høstetidspunkt til nåtid.
2. Inkrementer/nullstill antall ikke funnet.
3. Inkrementer/nullstill antall feil.
4. La dydbe være
5. Inkrementer/dekrementer sykluser/lengde

## Prosessering av utlenker

Gitt en kontekst med SEED, URL, COLLECTION, DEPTH, CYCLES, LENGTH, osv.
er følgende en algoritme for å behandle utlenker:

1. Hent alle profiler assosiert med SEED og som har samme COLLECTION.
2. Hver URL normaliseres og sjekkes i skopet (relativt til SEED) til alle profiler fra 1. steg:
    1. Hvis URL er i skopet og ikke ligger i kø så legges URL til i kø.
    2. Hvis URL er i skopet og ligger i kø med samme SEED oppdateres URL i kø.
    3. Hvis URL er i skopet og ligger i kø med annen SEED må det tas stilling til
       hvilken SEED som benyttes:

        1. URL er helt lik SEED i kontekst. Isåfall bruk SEED i kontekst.
           Spesifikke SEED trumfer.
        2. URL er helt lik SEED i kø. Isåfall bruk SEED i kø.
           Spesifikke SEED trumfer.
        3. URL deler domene med begge. Isåfall bruk den mest spesifikke SEED.
        4. URL deler domene med SEED i kontekst, men ikke med SEED i kø.
           Isåfall velg det som gir tidligste neste høstetidspunkt?
        5. URL deler domene med SEED i kø, men ikke med SEED i kontekst.
           Isåfall velg det som gir tidligste neste høstetidspunkt?
        6. URL deler ikke domene med noen av dem.
           Isåfall velg det som gir tidligste neste høstetidspunkt?
    4. Hvis URL ikke er i skopet og ligger i kø med samme SEED må det tas
       stilling til om URL skal fjernes fra kø eller ikke ved å sammenligne dybde ol.
    5. Hvis URL ikke er i skopet og ligger i kø med annen SEED; ignorer.
    6. Hvis URL ikke er i skopet og ikke ligger i kø så ignoreres URL.

Tabell som belyser problemstillingen som oppstår når en utlenke har forskjellig SEED i kontekst og i kø:

| Punkt | Seed URL (fra kontekst)            | Utlenke                      | Seed URL (fra kø)                  |
|-------|------------------------------------|------------------------------|------------------------------------|
| a.    | **_http://www.db.no/artikkel/en_** | http://www.db.no/artikkel/en | http://www.db.no                   |
| b.    | http://www.db.no/                  | http://www.db.no/artikkel/en | **_http://www.db.no/artikkel/en_** |
| c.    | http://www.db.no                   | http://www.db.no/artikkel/to | **_http://www.db.no/artikkel_**    |
| d.    | http://www.db.no                   | http://www.db.no/bla         | http://www.vg.no                   |
| e.    | http://www.vg.no                   | http://www.db.no/bla         | http://www.db.no                   |
| f.    | http://www.db.no                   | http://www.twatter.gov       | http://www.vg.no                   |

## Alternativ prosessering av utlenker (uten SEED i kø)
Følgende søker å finne ut av om det kan tenkes at SEED ikke er en del av kontekst fra URL kø.

Gitt en kontekst med URL, COLLECTION, DEPTH, CYCLES, LENGTH, osv.
er følgende en algoritme for å behandle utlenker:

1. Hent alle profiler assosiert med COLLECTION.
2. Hver URL normaliseres og sjekkes i skopet til alle profiler fra 1. steg.

Problem:
- Skopesjekk er relativt til noe (SEED). Noen av funksjonene i skopesjekk
bruker SEED som parameter (f.eks. `isSameHost`)
- Hvis det ikke finnes en SEED hva er dybde? Dybde er også relativ til SEED.    

Det kan tenkes at en teller holder rede på dydbe så lenge REFERRER er samme host.
Hvis utlenke er annet domene en REFERRER trer en annen teller i kraft som holder rede
på dybde OFF_SITE.
