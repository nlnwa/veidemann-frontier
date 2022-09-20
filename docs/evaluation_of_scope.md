# Evaluation of scope

## Definisjoner

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

## Hvordan starte høsting

- En seed startes i kontekst av en / flere profiler.
- En profil starter alle seed som er assosiert med seg selv i kontekst av seg selv
- En samling starter alle profiler som har den konfigurert.

## Starte seed (legge seed URL i kø)
Å starte en seed er det samme som å legge til en utlenke i kontekst
av profilens samling, med alle verdier nullstilt (dybde, osv.).

## Høste URL som ligger i kø
Gitt en kontekst med COLLECTION, URL, SEED, DEPTH, LENGTH, TIME_TO_LIVE, LAST_FETCH_TIME
er følgende en algoritme for å behandle URL som er valgt ut:

1. Hvis SEED ikke finnes fjernes URL fra kø.
2. Hvis COLLECTION ikke finnes fjernes URL fra kø. 
3. Hvis SEED er deaktivert legges URL tilbake i kø med neste høstetidspunkt i fremtiden.
4. Hvis COLLECTION er deaktivert legges URL tilbake i kø med neste høstetidspunkt i fremtiden.
5. Hent alle profiler assosiert med SEED og som har COLLECTION
6. Sjekk om URL er i skopet (relativt til SEED) hos alle profiler fra forrige steg:
   1. Hvis URL ikke er i skopet fjernes URL fra kø.
   2. Hvis URL er i skopet til bare deaktiverte profiler legges URL tilbake i kø med høstetidspunkt i fremtiden.
   3. Hvis URL er i skopet må det kanskje gjøres en dobbelsjekk mot profilenes frekvens 
   for å sikre at det er nødvendig å høste på nåvørende tidspunkt (f.eks. kan en profil ha fått langsommere frekvens siden URL ble lagt i kø).
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
2. Hver URL sjekkes i skopet (relativt til SEED) hos alle profiler fra 1. steg:
   1. Hvis URL er i skopet og ikke ligger i kø så legges URL til i kø.
   3. Hvis URL er i skopet og ligger i kø med samme SEED oppdateres URL i kø.
   4. Hvis URL er i skopet og ligger i kø med annen SEED må det tas stilling til
   om:
      1. URL er helt lik SEED i kontekst. Isåfall bruk SEED i kontekst.
      Spesifikke SEED trumfer.
      2. URL er helt lik SEED i kø. Isåfall bruk SEED i kø.
      Spesifikke SEED trumfer.
      3. URL deler domene med begge. Isåfall bruk den mest spesifikke SEED.
      4. URL deler domene med SEED i kontekst, men ikke med SEED i kø.
      Isåfall velg det som gir tidligste neste høstetidspunkt?
      2. URL deler domene med SEED i kø, men ikke med SEED i kontekst.
      Isåfall velg det som gir tidligste neste høstetidspunkt?
      3. URL deler ikke domene med noen av dem.
      Isåfall velg det som gir tidligste neste høstetidspunkt?

   5. Hvis URL ikke er i skopet og ligger i kø med samme SEED må det tas
   stilling til om URL skal fjernes fra kø eller ikke ved å sammenligne dybde ol.
   6. Hvis URL ikke er i skopet og ligger i kø med annen SEED; ignorer.
   7. Hvis URL ikke er i skopet og ikke ligger i kø så ignoreres URL.

Algoritmen ignorerer det faktum at profiler kan deaktiveres. Det må tas stilling til.
Hvis vi antar at bare aktiverte profiler hentes i 1. steg så får det konsekvensen at
URL i kø muligens slettes i steg IV. Kanskje er det ikke som er ønskelig.

## Notater
- En seed URL som er i skopet til profiler med ulik COLLECTION blir
  lagt inn i køen én gang for hver COLLECTION
- Lengde er bare navn på konspetet hvor mange ganger en URL skal høstes før den tas ut av køen.
