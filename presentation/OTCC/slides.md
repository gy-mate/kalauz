---
title: Mapping railway speed restrictions
author: Máté Gyöngyösi
exportFilename: railway-speed-restrictions_mapping_OTCC_Mate-Gyongyosi

theme: apple-basic
glowSeed: 11
aspectRatio: 16/9
transition: fade

contextMenu: false
lineNumbers: false

layout: intro-image
image: /SRs_Hegyeshalom.jpeg
---

# Mapping railway speed restrictions

Máté Gyöngyösi<br>
<i>@gymate:grin.hu</i>

<div class="absolute bottom-10">
  <span class="font-700">
    <i>Open Transport Community Conference<br>
    ÖBB Open Innovation Factory<br>
    Co-Creation Space<br>
    2025-10-18T14:00+02:00</i>
  </span>
</div>

<div style="position: absolute; bottom: 40px; right: 40px; text-align: right">
  <span class="font-700">
  </span>
</div>

---
layout: intro-image-right
image: /kalauz_GitHub.svg

transition: slide-left
---

https://github.com/gy-mate/kalauz/blob/main/presentation/OTCC/slides.md?plain=1

<!--
- hello everyone
- I'm Máté from Budapest, Hungary
- first of all, I'd just like to let you know that this is the link to the Markdown source code of this presentation
- all the references and details are in it
- I'll push the file right after the end of the session.
- 
- in the talk, I'd like to shortly introduce my app that maps railway speed restrictions
- it's located at the root of this repository.
-->

---
layout: iframe
url: https://openrailwaymap.app/#view=16.67/48.220983/16.393018&style=speed

transition: slide-up
---

<!--
- as some of you may know, OpenRailwayMap already shows operating speeds of railway lines
- but this talk won't be about those permanent ones
- my focus is on the "temporary" speed restrictions that can have a duration between hours and even decades
-->

---
transition: slide-left
---

<SlidevVideo controls autoplay>
  <source src="/snail-run.mp4" type="video/mp4"/>
  <p>
    Your browser does not support videos. You may download it
    <a href="/snail-run.mp4">here</a>.
  </p>
</SlidevVideo>

<!--
- so when I'm going to the Hungarian countryside by train, I'm just furious how long the journey takes and how slow we travel
- quite literally, a snail is often faster than the train (https://www.youtube.com/watch?v=1WAY1sFM6yQ)
- 
- there are also lots of videos on TikTok about tractor drivers overtaking the train
-->

---
layout: image
image: /derailed.png

transition: slide-left
---

<!--
- and we already got used to the derailments happening approximately every month (https://telex.hu/belfold/2025/09/12/mav-vonat-kisiklas-utas-magyarkut)
- but when for example there was a crane coming to rescue this exact derailed train a month ago...
-->

---
layout: image
image: /derailed-crane.jpeg

transition: slide-up
---

<!--
- ...and it also got derailed, I would call that extreme (https://telex.hu/belfold/2025/09/13/mav-a-daru-nem-kisiklott-csak-kerekei-rovid-idore-leugrottak-a-sinrol)
- so you can get a sense of the scale of the problem
- 
- so my motivation behind creating a map of slow zones is to visualize the catastrophic state of the Hungarian railway network
- partly for railway nerds like me
- but most importantly for putting pressure on politicians with it
- in order to make them properly fund the State Railways
- 
- so let's talk about the maps...
-->

---
layout: image
image: '/KOZOP_lassujelterkep.gif'
backgroundSize: contain

transition: slide-up
---

<!--
- ...the State Railways doesn't have even an internal one about slow zones
- neither do they have coordinates for them
- 
- the only one I could find about speed restrictions was this one from ~10 years ago (2014)
- and I knew that it didn't show the full picture at all
- so we submitted an FOI request to MÁV, the Hungarian State Railways...
-->

---
layout: image
image: /MAV_Kimittud_valasz.png

transition: slide-left
---

<!--
- ...which they denied
- so we sent a complaint to the informations commissioner...
-->

---
layout: intro-image-right
image: /NAIH_hatarozat.png

transition: slide-up
---

NAIH-4049-8/2023.

<!--
- ...and we fortunately got an official decision that speed restrictions are indeed public data
- therefore they have to make them available...
-->

---
transition: slide-left
---

<SlidevVideo controls autoplay>
  <source src="/MAV_SR-table.mp4" type="video/mp4"/>
  <p>
    Your browser does not support videos. You may download it
    <a href="/MAV_SR-table.mp4">here</a>.
  </p>
</SlidevVideo>

<!--
- ...which they did, to our surprise (e.g. here https://kimittud.hu/request/vasuti_sebessegkorlatozasok_2025#incoming-34648)
- and the dataset pretty much confirmed my suspicion
- 
- so if we sum the length of these speed restrictions...
- ...the Hungarian railway network is ~7,000 kilometres long
- and the length of the slow zones is ~7,600 kilometres
- 
- the latter number is higher because it includes all tracks' length, not just the network length
- but still, that's plenty
-->

---
zoom: 2

transition: slide-up
---

```py
>>> print(speed_restrictions[6])

'''
40 → 10 km/h speed restriction (#18e2c535) 
on line 100 
between metre posts 270200 and 270770 
(track 18 at Nyíregyháza) 
due to «Technical condition of the track» 
since internal provision 'Gy.638-0223' 
signed at 1985-02-14T00:00Z
'''
```

<!--
- so now we have all this data for each speed restriction
- this is actually the oldest one in the dataset
- it's there at Nyíregyháza since 1985 to this day
- and actually it's not the only one from those times.
- 
- so, now we have the data – how to put them on the map?
- first, I wrote a Python app that converts these speed restrictions to Excel and imports them to a database
- it also updates slow zones not in effect anymore accordingly
- so we have historical data
-->

---
layout: image
image: /milestones.png

transition: slide-up
---

<!--
- then, I added milestones to some of the railway lines (https://overpass-ultra.us/#m=11.59/47.9114/17.1646&q=NoIxHsA8C4G9bFAvkgugbgFADsDuAnYfAQwEsAbXYgTwF4BbCgUwGcAXcbJjTcAVzYACAOZNw9dEA)
- after that, I wrote a Python app that imports all railway data (incl. milestones) using an Overpass query
- 
- and as we don't have all milestones on the map, it interpolates all missing ones
- so basically you just need two milestones for an entire railway line:
- one at its beginning and one at its end
- 
- then you can match the slow zones with the map using metre posts
-->

---
layout: iframe
url: https://gy-mate.github.io/kalauz/

transition: slide-left
---

<!--
- and that's what I did – so the result looks like this
- red means that the line is impassable (0 km/h)
- orange means that the speed should be reduced to less than half of the operating speed
- 
- grey does not meen a lack of speed restrictions but a lack of milestones on that line
- so just imagine that all lines would basically look like these
- both the data and the matching are quite accurate
-->

---
layout: iframe
url: https://public.flourish.studio/visualisation/18623027/

transition: slide-up
---

<!--
- and as a fun addition I also categorized the causes of speed restrictions using machine learning
- on 4 category levels.
- 
- so, what next?
-->

---
layout: image
image: /Kanban.png

transition: slide-left
---

<!--
- currently I'm working on extending the map to all lines
- after that, I'll implement the visualization of speed restrictions on side tracks
- 
- you can track the progress on the Kanban board of the project
- and I'd be happy if you would open issues for your ideas
-->

---
layout: iframe
url: https://openrailwaymap.app/#view=13.63/52.51796/13.39503&date=1888
---

<!--
- my ultimate goal is to have a historical map view similar to the slider on OpenRailwayMap...
-->

---
layout: image
image: /megallj-jelzo.jpg

transition: slide-left
---

<!--
- ...extended with basically a git blame – so you can see who is responsible for its existence
- and maybe who you can contact to complain about it
-->

---
layout: iframe
url: https://gy-mate.github.io/kalauz/

transition: slide-up
---

<!--
- last but not least, adding foreign data for comparison would be awesome
- so please let me know if you have any knowledge or data of speed restrictions
- I'm already working on acquiring the ÖBB dataset
-->

---
layout: image
image: /megallj-jelzo.jpg

transition: slide-left
---

<!--
- thank you very much for your attention
- and I'll leave you with my favourite news headline from the last month
- which is about the same derailment I've shown you in the first slides
-->

---
layout: image
image: /derailed-crane_statement.png

transition: slide-up
---

<!--
- https://telex.hu/belfold/2025/09/13/mav-a-daru-nem-kisiklott-csak-kerekei-rovid-idore-leugrottak-a-sinrol
-->

---
layout: image
image: /MAV_typo_emails.png

transition: slide-up
---

<!--
- 12 állomáson 29 hiba a végrehajtási utasításokban
  - a kutatásom mellékhatásaként jeleztem az állomásfőnököknek, szakértőknek...
- ...akik ezt megköszönték, és jelezték, hogy javítani fogják őket
- a VPE torzított állomási helyszínrajzain pedig 35 állomáson 119 hibát találtam
  - nekik is jeleztem a pontatlanságokat
- így, hogy készen vannak a térképadatok, le lehet tölteni őket...
-->
