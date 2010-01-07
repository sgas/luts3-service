.PHONY: compile install clean

compile:
	true

install:
	python setup.py install --root ${DESTDIR}

clean:
	rm -rf _trial_temp MANIFEST build dist
	find . -name "*.pyc"|xargs rm -f
