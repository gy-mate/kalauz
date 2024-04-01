---
theme: apple-basic

layout: intro-image
image: '/SRs_Hegyeshalom.jpeg'

transition: slide-left
---

<div class="absolute top-10">
	<span class="font-200" font-size="3.8 pt">
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

<!--suppress HtmlDeprecatedAttribute -->
<div align="right">
	<span class="font-200" font-size="3.8 pt">
		Szekció neve<br>
        2024. 05. 29. 1x:xx
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

# Kutatási terv

rövid összefoglalás

---
transition: slide-up
---

# Teszt

```python
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