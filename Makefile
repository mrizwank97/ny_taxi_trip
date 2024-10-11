# Define variables
FILE_ID=11TaQawXae8YRIhBs_TnJE7YzriXFul-7
OUTPUT=backup.zip

# Default target
all: download unzip start

start:
	docker compose up --build -d

# Download the zip file from Google Drive
download:
	@echo "Downloading file..."
	wget --no-check-certificate 'https://drive.google.com/uc?export=download&id=$(FILE_ID)' -O $(OUTPUT)

# Unzip the downloaded file
unzip:
	@echo "Unzipping file..."
	unzip -o $(OUTPUT)

# Clean up zip file
clean:
	@echo "Cleaning up..."
	rm -f $(OUTPUT)

# Phony targets
.PHONY: all download unzip clean

# call the docker down and zip the directory 'persistent_db' to backup.zip
shutdown:
	docker compose down
	zip -r backup.zip persistent_db