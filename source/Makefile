tags:
	rm -f TAGS
	find . -type f -name '*.py' -print0 | xargs -0 etags --append


clean:
	find . -name '*~' -exec rm {} \;


find_caps:
	find . -name '*.py' -print0 | xargs -0 egrep '[^A-Za-z][A-Z]([A-Z_]+)[^a-zA-Z]' | grep -v 'C[.]' | grep -v /ctools | grep -v /dfxml | grep -v /constants.py


e601:
	pylint `pwd` --rcfile=.pylintrc -j 8 --enable=E0601


e602:
	pylint `pwd` --rcfile=.pylintrc -j 8 --enable=E0602

master:
	git submodule foreach --recursive 'git checkout master;git pull'

das2020_certificate.pdf: das2020_certificate.tex
	if [ ! -e Seal_of_the_United_States_Census_Bureau.pdf ]; then ln -s das_framework/certificate/Seal_of_the_United_States_Census_Bureau.pdf; fi
	if [ ! -e background1.jpg ]; then ln -s das_framework/certificate/background1.jpg; fi
	TEXINPUTS=.:das_framework/certificate/texfiles: pdflatex -interaction nonstop das2020_certificate.tex
	cp das2020_certificate.pdf $$HOME/public_html/
