RELEASE:=das_decennial_e2e.tar.gz

help:
	@echo 'type "make release" to make the release'
	@echo 'type "make checkrelease" to make a release and check it in a temp directory'

release:
	make clean
	tar --exclude='*~' --exclude='.*' --exclude '__pycache__' --exclude '*.pyc' --create --gzip --file=${RELEASE} das_decennial
	@ls -l ${RELEASE}
	@echo Files in release: `tar tfvz ${RELEASE} | wc -l`

${RELEASE}: release

checkrelease: ${RELEASE}
	(/bin/rm -rf temp$$$$; mkdir temp$$$$; cd temp$$$$; tar xpBf ../${RELEASE};cd das_decennial;export S3DEST=$$DAS_S3ROOT/tmp/e2erelease_test$$$$;bash run_1940.sh)


clean:
	find . -name '*~' -exec rm {} \;
	/bin/rm -f ${RELEASE}

