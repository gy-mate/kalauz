---
theme: apple-basic

layout: intro-image
image: '/SRs_Hegyeshalom.jpeg'

transition: slide-left
---

<div class="absolute top-10">
	<span class="font-200">
		<b>Kutató:</b><br>
		Gyöngyösi Máté szociológus,<br>
		üzemmérnök-informatikus BProf hallgató<br>
		Villamosmérnöki és Informatikai Kar<br><br>
		<b>Témavezető:</b><br>
		Dr. Orosz Csaba egyetemi docens<br>
		Építőmérnöki Kar<br>
		Út és Vasútépítési Tanszék
	</span>
</div>

<div style="text-align: right">
	<span class="font-200">
        2024. 05. 29. 10:20<br>
		Építőmérnöki tudományok<br>
        2. szekció
	</span>
</div>

<div class="absolute bottom-10">
	<h1>Sebességkorlátozások</h1>
	<p>jellemzőinek elemzése a magyarországi vasúthálózaton<br>
mesterséges intelligenciával és geoinformatikai módszerekkel</p>
</div>


---
transition: slide-left
---

<!--suppress HtmlUnknownTag -->
[//]: # (<SlidevVideo controls=true autoPlay=true autoPause="slide" autoReset="slide">)
[//]: # (  <!--suppress HtmlUnknownTarget -->)
[//]: # (<source src="/Homolya_eloadas.mp4" type="video/mp4"/>)
[//]: # (  <p>)
[//]: # (    Your browser does not support videos. You may download it)
[//]: # (    <!--suppress HtmlUnknownTarget -->)
[//]: # (    <a href="/Homolya_eloadas.mp4">here</a>.)
[//]: # (  </p>)
[//]: # (</SlidevVideo>)


---
transition: slide-up
---

# Kutatási terv

- lassújelek: MySQL-adatbázis
- közérdekűadat-igénylés
  - MÁV: TopoRail
  - GySEV: állomási vázlatos helyszínrajzok
- vasútvonalak objektumainak
  - feltöltése az OpenStreetMapre
  - leszűrése és letöltése az OpenStreetMapről
- lassújelek térképalapra illesztése
- interaktív térképes megjelenítés
- összefoglaló a szakirodalomról
- elemzés az okokról
- összefoglaló a megoldási lehetőségekről


---
transition: slide-up

layout: iframe
url: https://gy-mate.github.io/kalauz/
---

# Teszt

```python
import geojson

def visualise_srs(self) -> None:
        features = []
        for i, node in enumerate(self.osm_data.nodes):
            point = geojson.Point((float(node.lon), float(node.lat)))
            node.tags |= {"line_color": [0, 0, 0, 0]}
            feature = geojson.Feature(
                geometry=point,
                properties=node.tags,
            )
            features.append(feature)
```


---
transition: fade-out
---

# Eredmények

és azok összevetése a vállalásokkal


---
layout: quote
---

A kutatás a Kulturális és Innovációs Minisztérium *ÚNKP-23-1-I-BME-354* kódszámú Új Nemzeti Kiválóság Programjának a Nemzeti Kutatási, Fejlesztési és Innovációs Alapból finanszírozott szakmai támogatásával készült.

![ÚNKP-, NKFI- & KIM-logo](/funding.svg)

