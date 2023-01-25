build:
	rm -rf Destination
	mkdir Destination
	cp Code/main.py ./Destination
	cp Code/config.yaml ./Destination
	unzip Data.zip -d Destination/
	cd Code/src && zip -r ../../Destination/src.zip .
	rm -rf Destination/__MACOSX
