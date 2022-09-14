# Use cases

## Flere språk

### Eksempel 1: [Sametinget](https://sametinget.no) (cookie)

Språk settes ved å velge fra meny (javascript) eller ved å legge parameter `sprak=nn` til url.
Det resulterer i en verdi i cookie: `lang=nn`.

Parameter for språk er ikke en del av interne lenker videre så cookie bestemmer
da språk.

#### Seeds
1. https://sametinget.no/
2. https://sametinget.no/?sprak=12
3. https://sametinget.no/?sprak=14
4. https://sametinget.no/?sprak=15

Kan __ikke__ høstes til samme kolleksjon fordi URLer vil kunne være tvetydig.

For eksempel vil https://sametinget.no/om-sametinget/ gi forskjellig innhold
basert på hvilket språk som er angitt i cookie.

### Eksempel 2: [Karasjok kommune](https://www.karasjok.kommune.no/) (cookie)

Lignende plattform som Sametinget, men her er funksjonen for å endre språk en lenke
og ikke gjemt bak javascript meny som i tilfellet Sametinget.

##### Seed
1. https://www.karasjok.kommune.no/ (setter implisitt sprak til norsk i cookie)
2. https://www.karasjok.kommune.no/hovedportal/?sprak=12 (samisk)
3. https://www.karasjok.kommune.no/hovedportal/ (samme som 2.)

Høsting av flere språk kan gjøres i samme kolleksjon da ingen URL er tvetydig.




#### Eksempel 3: [Stortinget](https://stortinget.no) (prefiks)

Språk er bestemt av en prefiks til sti:
1. /no/
2. /nn/
3. /en/

Det er lenker til alle språk på forsiden og alle interne lenker inkluderer
prefiks.

Kan høstes til samme kolleksjon.

### Spørsmål

1. Hvordan høster vi alle språk hvis det lenkes til en host som har flere språk?
