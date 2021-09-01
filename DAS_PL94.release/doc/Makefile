figs:
	echo Making figures using pyreverse
	(cd ../..;pyreverse -o pdf -c das_decennial.programs.queries.make_queries.QueriesCreator das_decennial)
	(cd ../..;pyreverse -o pdf -c das_decennial.programs.queries.querybase.AbstractLinearQuery das_decennial)
	(cd ../..;pyreverse -o pdf -c das_decennial.programs.nodes.nodes.GeounitNode das_decennial)
	(cd ../..;pyreverse -o pdf -c das_decennial.das_framework.driver.AbstractDASModule das_decennial)
	(cd ../..;pyreverse -o pdf -c das_decennial.programs.engine.topdown_engine.TopdownEngine das_decennial)
	(cd ..;pyreverse -o pdf programs/engine/hdmm_engine.py programs/engine/topdown_engine.py programs/engine/bottomup_engine.py programs/engine/hdmm_errors_engine.py programs/engine/engine_utils.py; cd -; mv ../classes.pdf engines_hierarchy.pdf; rm ../packages.pdf)
	mv ../../*.pdf .
#	(cd ..;pyreverse programs/engine/hdmm_engine.py programs/engine/topdown_engine.py programs/engine/bottomup_engine.py programs/engine/hdmm_errors_engine.py programs/engine/engine_utils.py; cd -; dot -Tpdf ../classes.dot -o engines_hierarchy.pdf; rm ../packages.dot ../classes.dot)
