# Use cases

## Høste nettsted på flere språk

### Konklusjon
Det kommer ann på hvilket nettsted det er om det er behov for å høste forskjellige språk til forskjellige collection eller ikke. 

### Eksempel 1: [Sametinget](https://sametinget.no) (cookie)

Språk settes ved å velge fra meny (som bruker javascript) eller ved å legge parameter `sprak=nn` til url.
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

#### Seed
1. https://www.karasjok.kommune.no/ (setter implisitt sprak til norsk i cookie)
2. https://www.karasjok.kommune.no/hovedportal/?sprak=12 (samisk)
3. https://www.karasjok.kommune.no/hovedportal/ (samme som 2.)

Samisk språk bruker prefikset `/hovedportal/` så høsting av flere språk kan gjøres i samme kolleksjon da ingen URL er tvetydig.

### Eksempel 3: [Stortinget](https://stortinget.no) (prefiks)

Språk er bestemt av en prefiks til sti:
1. /no/
2. /nn/
3. /en/

Det er lenker til alle språk på forsiden og alle interne lenker inkluderer
prefiks.

Kan høstes til samme kolleksjon.

## [Høste nettsted inkludert kontekst som omfatter andre nettsteder](#kontekst)

### Eksempel: [Faktisk.](https://www.faktisk.no/)

URL:
https://www.faktisk.no/artikler/0rg41/hvor-mye-strom-selger-vi-til-utlandet

```javascript
// Document link references
Array.from(document.links).map(_ => _.href)
  // but only http/https links
  .filter(_ => _.startsWith("http"))
  // and not internal links.
  .filter(_ => ! _.startsWith("https://www.faktisk.no/"))

// Output
[
  "https://www.ssb.no/energi-og-industri/energi/statistikk/elektrisitet/artikler/tidenes-hoyeste-krafteksport-i-2021",
  "https://www.tu.no/artikler/her-er-alle-norges-utenlandskabler/513908?key=CBQpXqRD",
  "https://www.tv2.no/nyheter/innenriks/ber-kraftprodusentene-spare-pa-vannet-til-vinteren/15059943/",
  "https://www.tu.no/artikler/statnett-tjente-fire-milliarder-pa-utenlandskablene-i-2021/516293",
  "https://www.ssb.no/energi-og-industri/energi/statistikk/elektrisitet/artikler/tidenes-hoyeste-krafteksport-i-2021",
  "https://www.nordpoolgroup.com/en/maps/#/nordicc",
  "https://www.nrk.no/nordland/strom_-midt-norge-og-nord-norge-star-for-80-prosent-av-stromeksporten-1.16101718",
  "https://www.nrk.no/nordland/stromprisene-stiger-raskt-i-midt-norge-og-nord-norge-_-ekspertene-anbefaler-fastpris-1.16096744",
  "https://energiogklima.no/to-grader/ekspertintervju/ekspertintervjuet-slik-virker-kraftmarkedet/",
  "https://www.nve.no/media/14410/oppsummering-av-innrapportert-produksjon-i-soerlige-norge-no1-no2-og-no5-uke-35.pdf",
  "https://www.nve.no/nytt-fra-nve/nyheter-energi/norge-importerte-mer-enn-vi-eksporterte-i-2019-men-bare-sa-vidt/",
  "http://presse.no/pfu/etiske-regler/vaer-varsom-plakaten/",
  "https://www.nored.no/Redaktoeransvar/Redaktoerplakaten",
  "https://www.facebook.com/faktisk",
  "https://twitter.com/faktisk_no"
]
```

I paradigmet med deklarativ innhøsting vil utlenker bli lagt i kø hvis
det finnes en seed med profil som har utlenke i kikkerten ("scope").

Utlenker kan havne i samme eller andre kolleksjoner?

[![](https://mermaid.ink/img/pako:eNpdkUFu3DAMRa8iKEBgA-F4POjKiyyKphdIltrII9pWJEsDkZ7CCHKcnKQXKz1wUrQ7Uvri-_x60-fsUHd6iPnXebKF1fcXk0yipR-LvUzqpw3sKRxMGo5VVQ17m3Jd1wCPQ1vdNfLMh4ilOZbxW9tM11xgXhGIS56BMI5Y4OqBfYSFo00O-a5WJmFy_8Ce2cr0DUCYuNjYr-X3h0l9K2iifsceDvDYnyqjG0xYRg95BJ_cIjy_HzW0jwqhwYhBbsgz8l-v7J1ICaa8IjFCKHZgDHTJhcHD6Xhqja6_POZMrRAn5gt1zU7JY4h-tmKr4Qyyg5PB2wwUSmIs19fl_x4ZKPogeZQgsdyws5VaQmluROI1otqAakswIDhL8jfFrp1qj5uboZXsVX9S9zedSfpBz1hm65185ptJShnNE85odCelE4DRJr2Lbrk4y_jkPOeiu8FGwgdtF87PazrrjsuCn6If3spS8656_wOIocht)](https://mermaid.live/edit#pako:eNpdkUFu3DAMRa8iKEBgA-F4POjKiyyKphdIltrII9pWJEsDkZ7CCHKcnKQXKz1wUrQ7Uvri-_x60-fsUHd6iPnXebKF1fcXk0yipR-LvUzqpw3sKRxMGo5VVQ17m3Jd1wCPQ1vdNfLMh4ilOZbxW9tM11xgXhGIS56BMI5Y4OqBfYSFo00O-a5WJmFy_8Ce2cr0DUCYuNjYr-X3h0l9K2iifsceDvDYnyqjG0xYRg95BJ_cIjy_HzW0jwqhwYhBbsgz8l-v7J1ICaa8IjFCKHZgDHTJhcHD6Xhqja6_POZMrRAn5gt1zU7JY4h-tmKr4Qyyg5PB2wwUSmIs19fl_x4ZKPogeZQgsdyws5VaQmluROI1otqAakswIDhL8jfFrp1qj5uboZXsVX9S9zedSfpBz1hm65185ptJShnNE85odCelE4DRJr2Lbrk4y_jkPOeiu8FGwgdtF87PazrrjsuCn6If3spS8656_wOIocht)

### Eksempel: [Wikipedia](https://no.wikipedia.org/)

#### [Skredet i Gjerdrum](https://no.wikipedia.org/wiki/Leirskredet_i_Gjerdrum_2020)

Formål: Event som fordrer å høste side på wikipedia inkludert referanser


[![](https://mermaid.ink/img/pako:eNplUV1r3DAQ_CtCULiDrH2-0hc_FBoSAqW0D7nSPgiOzWl9VqwPdyVXmJD_Xtl3KZRKL5J2ZnZG-yJPQZNsZWdDPvXISdwelFc-Tk9nxrEX97_Jp1bEgUlTAgPnZ2LNk1NelEW52Ww2StbZDKb-QoavyKM5PlyRx_1uv1Nyu91eON3Kqcl5qlf0WiGv_2l8oMGbOIjvAz1ZLLW0K6ycc5WmyofCgQo-Fq1F8n_611CciB_F1UjaoPJ5oftQ5benKvD5qlJS_FVYNu1DiE2J1ac0xrau17ZkLTHTGDhVp-BqTznWSzZo9vC-gXe7TwDgkA1C6E1EGwA64y16DSYCE-oZUoCe7Ag-cMYZTHe5rvVfE8VEehWqHj__7Ml9ez7cVX1ytnxgiZdmS-JiT8TEYSDQGMvgGOdWfFjN5wZKpBWjvLyRjtih0WXKL8sAlExFl5Rsy1EjD0oq_1pw06gx0b02KbBsO7SRbiROKTzO_iTbxBO9ge4Mlm92V9TrH4QEx0w)](https://mermaid.live/edit#pako:eNplUV1r3DAQ_CtCULiDrH2-0hc_FBoSAqW0D7nSPgiOzWl9VqwPdyVXmJD_Xtl3KZRKL5J2ZnZG-yJPQZNsZWdDPvXISdwelFc-Tk9nxrEX97_Jp1bEgUlTAgPnZ2LNk1NelEW52Ww2StbZDKb-QoavyKM5PlyRx_1uv1Nyu91eON3Kqcl5qlf0WiGv_2l8oMGbOIjvAz1ZLLW0K6ycc5WmyofCgQo-Fq1F8n_611CciB_F1UjaoPJ5oftQ5benKvD5qlJS_FVYNu1DiE2J1ac0xrau17ZkLTHTGDhVp-BqTznWSzZo9vC-gXe7TwDgkA1C6E1EGwA64y16DSYCE-oZUoCe7Ag-cMYZTHe5rvVfE8VEehWqHj__7Ml9ez7cVX1ytnxgiZdmS-JiT8TEYSDQGMvgGOdWfFjN5wZKpBWjvLyRjtih0WXKL8sAlExFl5Rsy1EjD0oq_1pw06gx0b02KbBsO7SRbiROKTzO_iTbxBO9ge4Mlm92V9TrH4QEx0w)
