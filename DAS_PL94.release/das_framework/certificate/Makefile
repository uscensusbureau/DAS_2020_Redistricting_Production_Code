ALL = cert_template.pdf

all: $(ALL)

cert_template.pdf: cert_template.tex
	TEXINPUTS=.:texfiles: pdflatex cert_template.tex

clean:
	/bin/rm -f *.{bak,aux,log,fls,sh,stderr,stdout,fdb_latexmk}
	/bin/rm -f *~
	/bin/rm -f $(ALL)
