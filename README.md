# DiVA_kolleKTHor-Scopus


***

## 1. Kort beskrivning

**Detta program läser en KTH DiVA‑export (CSV) för ett valt år/intervall, hittar publikationer som saknar ScopusId, och försöker fylla på dessa genom att fråga Scopus Search API i två steg för att hitta respektive publikationens Scopus EID (t.ex. `2-s2.0-105028411355`).**

Skriptet är tänkt som ett **syskon** till Crossref‑skriptet i [**DiVA_kolleKTHor-CR**](https://github.com/awandahl/DiVA_kolleKTHor-CR)‑projektet, samt till WoS‑skriptet i [**DiVA_kolleKTHor-WoS**](https://github.com/awandahl/DiVA_kolleKTHor-WoS):

- Samma grundstruktur och kodlayout.
- Liknande kolumnnamn och utdataformat.
- Både **CSV** och **Excel** med klickbara länkar till DiVA och Scopus.

***

## 2. Huvudfunktioner

**Tvåstegad Scopus‑uppslagning för saknade ScopusId/EID:**

1. **Runda 1 – DOI → Scopus EID**
    - Filtrerar fram poster som:
        - ligger inom angivet år/intervall,
        - har en **DOI**,
        - saknar **ScopusId**.
    - Frågar **Scopus Search API** med en DOI‑sökning av typen
`query=doi(<doi>)` (t.ex. `query=doi(10.1609/aimag.v36i2.2590)`), och hämtar dokumentets **`eid`** (Scopus EID) om träff finns.
    - Matchade poster får ett **verifierat Scopus EID**.
2. **Runda 2 – Titel + år → Scopus EID**
    - Körs endast på de poster som fortfarande saknar verifierad Scopus EID efter runda 1.
    - Frågar Scopus med kombinationer av t.ex. **titel (TITLE)** och **publiceringsår (PUBYEAR)**, t.ex.
`TITLE("...") AND PUBYEAR = YYYY`.
    - Använder liknande typ av **verifieringslogik** som Crossref‑ och WoS‑skripten:
        - titel‑likhet,
        - publikationstyp (mapping mot Scopus `subtype` / `subtypeDescription` / `prism:aggregationType`),
        - ISSN/ISBN, volym, nummer, sidor (`prism:issn`, `prism:volume`, `prism:issueIdentifier`, `prism:pageRange`),
        - författar‑efternamn (när tillgängligt, via `dc:creator` / `author.surname`).
    - Ger antingen **Verified_Scopus_EID** eller **Possible_Scopus_EID** beroende på hur stark matchningen är.

**Utdata:**

- En **CSV‑fil** med originaldata från DiVA plus nya kolumner för bl.a.:
    - `Verified_Scopus_EID`
    - `Possible_Scopus_EID`
    - `Verified_Scopus_DOI`
    - `Possible_Scopus_DOI`
    - eventuella kommentars-/statusfält (t.ex. hur matchen hittades)
- En **Excel‑fil** med:
    - samma kolumner som CSV,
    - automatiskt genererade **hyperlänkar** till:
        - **DiVA‑posten** för varje rad,
        - motsvarande **Scopus‑post** när Scopus EID hittats.

***

## 3. Typiskt arbetsflöde

1. **Exportera data från KTH DiVA**
    - Gör en **CSV‑export** för önskat år eller årsspann.
    - Se till att kolumner som **DOI, ISI, ScopusId, PMID, titel, år, publikationstyp, ISSN/ISBN, volym, nummer, sidor, författare** följer den struktur som både Crossref‑, WoS‑ och Scopus‑skripten förväntar sig.
2. **Kör Scopus‑skriptet mot CSV‑filen**
    - Ange:
        - **in‑fil** (DiVA‑CSV),
        - **ut‑filnamn** för CSV/Excel,
        - **år eller årsspann**,
        - **Scopus API‑nyckel** (`X-ELS-APIKey`),
        - eventuellt begränsningar/rate‑delay beroende på konfiguration.
3. **Granska resultatet**
    - Öppna **Excel‑filen**.
    - Klicka igenom **DiVA‑länkarna** och **Scopus‑länkarna** för att manuellt granska gränsfall.
    - Använd kolumner som `Verified_Scopus_EID` respektive `Possible_Scopus_EID` för att se var matchningen är stark respektive osäker.

***

## 4. Förväntad målgrupp och användningsfall

Detta skript är riktat till:

- Bibliotekarier, metadata‑arbetare och bibliometrer vid KTH (eller andra DiVA‑anslutna lärosäten) som vill:
    - komplettera DiVA‑poster med **saknade ScopusId/EID**,
    - förbereda data för **citeringsanalys** och andra bibliometriska studier där Scopus används,
    - få en reproducerbar, skriptbaserad process parallell med Crossref‑baserade DOI‑kompletteringar och WoS‑baserade ISI‑kompletteringar.

Programmets design gör det lämpligt att:

- köras **årsvis** eller för valda spann av år,
- integreras i en återkommande **datakvalitetsrutin**,
- jämföras sida vid sida med Crossref‑ och WoS‑skripten i samma **DiVA_kolleKTHor**‑miljö.

***

## 5. Installation och beroenden (översikt)

Skriptet är implementerat i **Python 3** och använder vanliga paket för datahantering och HTTP‑anrop.

Typiska beroenden:

- **pandas** – CSV‑/Excel‑hantering
- **requests** – API‑anrop mot Scopus Search API
- **tqdm** – progressbar i terminalen
- **datetime** – paket för datum/tid
- **xlsxwriter** – paket för Excel‑skrivning

***

## 6. Konfiguration

De viktigaste inställningarna ligger i toppen av skriptet:

- **Input‑fil:** sökväg till DiVA‑CSV
- **Output‑filer:** basnamn för CSV + Excel
- **År / årsspann:** filtrerar vilka DiVA‑poster som behandlas
- **Scopus API‑nyckel:** `X-ELS-APIKey` för Scopus Search API
- **Rate limiting:** eventuell paus mellan anrop för att vara snäll mot API:et

***

## 7. Relation till syskonskripten (Crossref och WoS)

Detta Scopus‑skript är tänkt att:

- komplettera Crossref‑skriptet i [**DiVA_kolleKTHor-CR**](https://github.com/awandahl/DiVA_kolleKTHor-CR)‑projektet,
- komplettera WoS‑skriptet i [**DiVA_kolleKTHor-WoS**](https://github.com/awandahl/DiVA_kolleKTHor-WoS),
- dela samma **struktur, kolumnupplägg och filosofier för matchningslogik**,
- möjliggöra en **sammanhängande kedja**:

1. Crossref‑skriptet: hitta/förbättra **DOI** för poster utan externa ID.
2. WoS‑skriptet: utifrån DOI (och vid behov titel/år) fylla i **saknade ISI‑ID/WOS UID**.
3. Scopus‑skriptet: utifrån DOI (och vid behov titel/år) fylla i **saknade ScopusId/EID**.

***

## 8. License

This project is licensed under the MIT License.

Copyright (c) 2025 Anders Wändahl

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

***
