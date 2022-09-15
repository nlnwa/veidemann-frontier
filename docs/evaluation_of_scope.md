# Evaluation of scope

## Finn og velg profil til URL

Gitt en URL (og en COLLECTION), hvordan finne og velge profil (på en effektiv måte)?

1. Søk i seedliste etter eksakt treff på URL.
   
   Hvis treff gå til 4.
2. Søk i seedliste etter mer og mer generelle varianter av URL
   (ta bort parameter, ta bort sti, ta bort subdomener, etc).
   
   Hvis treff gå til 4.
3. Ikke treff i seedliste: (Domenet til) URL legges til i seedlisten (uten profil, eller standard profil?, hvilken entitet?). 
4. Sjekk scope for alle profiler (som matcher COLLECTION) (i en eller annen rekkefølge?)
   og velg en som matcher (hvilken?). 

``` go
func FindProfiles(URL, COLLECTION) PROFILES
    FOR EACH Variant IN url_variants_in_order_of_specificity(URL)
      SET Seed = SEARCH seedlist by Variant  
      IF Seed THEN return Seed.Profiles
    return empty list
    
func CheckScope(URL, COLLECTION, PROFILES)
    FOR EACH Profile IN PROFILES
      IF NOT Profile.Collection EQUALS COLLECTION
      THEN CONTINUE
      ELSE SET Pass = Profile.CheckScope(URL)

// Find profile for http://seed.se in default collection
SET Url = "http://seed.se"
SET Collection = "default"

SET Profiles = FindProfiles("http://seed.se", "default")

```

### Organisk vekst av seedliste
Ved hjelp av algoritmen over vil alle domener til alle URLer, som passerer scope sjekk i en eller annen profil,
automatisk legges til i seedliste.

## Optimisering
