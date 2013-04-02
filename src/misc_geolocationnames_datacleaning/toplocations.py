from bs4 import BeautifulSoup

def tolower():
	f = open('topcities.txt', 'r')
	cities = f.readlines()
	f.close()
	refs = get_top_us_cities()
	locations = set()
	for i in cities:
		locations.add(i.strip().lower())
	locations = refs | locations
	f = open('toplocations.txt', 'w')
	locations = locations - set(['salt lake', 'fond'])
	for l in locations:
		f.write(l+'\n')
	f.close()

def top200():
	f = open('extralocations.txt', 'r')
	world_cities_list = set([x.strip().lower() for x in f.readlines()])
	refs = get_top_us_cities(200)
	world_cities_list = world_cities_list | refs
	f = open('top200locations.txt', 'w')
	for l in world_cities_list:
		f.write(l+'\n')
	f.close()

def get_top_us_cities(n=None):
	src = 'Biggest_US_Cities_Top1000_2009_Census_data.html'
	f = open(src, 'r')
	l = f.read()
	bs = BeautifulSoup(l)
	bs.prettify()
	trs = bs.find_all('tr')
	locations = set()
	for i in trs:
		tds = i.find_all('td')
		if len(tds) > 3:
			for j in tds[1].contents:
				locations.add(j.strip().lower())
			for j in tds[3].find('a').contents:
				locations.add(j.strip().lower())
			if n != None:
				n -= 1
			if n == 0:
				break
	return locations

if __name__ == '__main__':
	#tolower()
	top200()
