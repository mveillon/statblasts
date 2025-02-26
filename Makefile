fmt:
	black src main.py

lint:
	flake8 src main.py

build:
	python main.py build -s 1900

publish:
	python main.py publish -s 1900
	