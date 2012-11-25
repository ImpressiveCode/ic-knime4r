#
# ------------------------------------------------------------------------
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to majchmar@gmail.com
# ------------------------------------------------------------------------

# Prepars Knime workflow data
# Params:
# wvar	- workflow variable name, required.
# value - workflow variable value, optional.
knime.wdata <- function(wvar = NA, value = NULL){
	if(is.na(wvar)){
		stop("wvar has to be defined.") 
	}
	list(wvar=wvar, value=value, type=wvar_type(value))
}

# Knime workflow executor
# Params:
# 	workflow 	 - knime workflow absolute path, zip or directory
# 	r2k_wvars	 - list of workflow variables. Workflow variable has to be prepared using knime.wdata function.
# 	k2r_wvars	 - list of workflow variables. Workflow variable has to be prepared using knime.wdata function. 
# 	executable   - knime.exe location, by default knime register knime in PATH
# returns  - vector of tabular data obtained from knime workflow. Please note that param k2r_wvars defines outgoing workflow variables.
knime.execute <- function(workflow = NA, r2k_wvars=list(), k2r_wvars=list(), executable='knime', showConsole=TRUE) {
	#base knime execution params, default for every knime execution
	shell_cmd = '-nosave -nosplash -reset -failonloaderror -application org.knime.product.KNIME_BATCH_APPLICATION';
	
	#add extra params
	if(showConsole){
		shell_cmd = paste("-consoleLog", shell_cmd);
	}
	
	#add workflow location
	shell_cmd = paste(shell_cmd, 
						ifelse(
							is.na(workflow), 
							stop("workflow has to be set"), 
							paste0('-workflowDir=', workflow)
						)
					)
	#prepare workflow variables
	workflow_r2k_vars = lapply(r2k_wvars, do_with_r2k_wvar)
	workflow_k2r_vars = lapply(k2r_wvars, do_with_k2r_wvar)

	variable_params = paste(
						paste(lapply(workflow_r2k_vars, convert_to_cmd)), 
						paste(lapply(workflow_k2r_vars, convert_to_cmd))
					)

		
	#finally the execution string
	shell_cmd = paste(executable, shell_cmd, variable_params);   
	print(paste("Executing:",shell_cmd));
	
	#execute 
	x <- shell(shell_cmd);
	if(x!=0){
		stop("Workflow execution failed.")
	} else {
	#after successful execution we can read data stored in tmp files
	lapply(workflow_k2r_vars, function(x)read.csv(file=x$value));
	}
}

# Knime workflow executor
# Params:
#   pmml		 - PMML model
#	pmml_wvar_name - PMML workflow variable name
# 	workflow 	 - knime workflow absolute path, zip or directory
# 	r2k_wvars	 - list of workflow variables. Workflow variable has to be prepared using knime.wdata function.
# 	k2r_wvars	 - list of workflow variables. Workflow variable has to be prepared using knime.wdata function. 
# 	executable   - knime.exe location, by default knime register knime in PATH
# returns  - vector of tabular data obtained from knime workflow. Please note that param k2r_wvars defines outgoing workflow variables.
knime.execute_pmml <- function(workflow, pmml, pmml_wvar_name, r2k_wvars=list(), k2r_wvars=list(), executable='knime', showConsole=TRUE) { 
	
	#requires XML package
	library(pmml)
	library(XML)
	#force update to PMML version 4.0
	upmml <- pmml
	xmlAttrs(upmml)[1] <- "4.0" #version atrtribute
	xmlAttrs(upmml)[2] <- "http://www.dmg.org/PMML-4_0" #xmlns
	xmlAttrs(upmml)[4] <- "http://www.dmg.org/PMML-4_0 http://www.dmg.org/v4-0/pmml-4-0.xsd" #xmlns:location
	
	#print(toString(upmml))
	
	#saves temporary pmml to tmp directory
	tmpfile <- tempfile();
	saveXML(upmml, tmpfile)
	#execute using base function
	x <- knime.execute(workflow, append(list(knime.wdata(pmml_wvar_name, tmpfile)), r2k_wvars), k2r_wvars, executable, showConsole)
	return(x);
}

########################################################
#Help functions, should not be used directly ###########
########################################################
convert_to_cmd <- function(wvar) {
	paste0("-workflow.variable=", paste(wvar, collapse=","))
}

do_with_r2k_wvar <- function(wvar){
	value= ifelse(exchange_as_file(wvar$value), write_tmp_file(wvar$value), wvar$value)
	knime.wdata(wvar$wvar, value)
} 

do_with_k2r_wvar <- function(wvar){
	value <- tempfile(); #create empty file, for data exchange
	knime.wdata(wvar$wvar, value)
} 

wvar_type <- function(value){
	switch(typeof(value), double = "double", character = "String", list = "String", integer = "int", NULL = "String", "String");
}

exchange_as_file <- function(value){
	switch(typeof(value), list = TRUE, FALSE)
}

write_tmp_file <- function(value) {
	#prepare file for data exchange
	tmpfile <- tempfile();
	#save temporary data
	write.csv(file=tmpfile, value, row.names = FALSE);
	#return file name
	tmp_urlfile = paste("file:///",tmpfile, sep=""); # url protocol becouse of csv reader nature
}
