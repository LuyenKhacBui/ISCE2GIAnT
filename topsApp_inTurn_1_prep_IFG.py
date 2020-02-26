#!/usr/bin/env python
import argparse
import subprocess
import os, sys, glob
from datetime import date
import datetime
import xml.etree.ElementTree as ET

''' This is a tool used to run topsApp.py from 'startup' to 'computeBaselines' for choosing IFGs that will be processed based on SBAS approach.
	This script will run for IFGs between 1st image and the others.
	Note that:	this will run topsApp.py to 'computeBaselines' step for IFGs only, not choosing IFGs to be subsequently processed in full.
				For this choosing, run script: topsApp_inTurn_2_choose_IFG.py
	This is coded by Luyen BUI on: 05-06 JUNE 2019'''

def cmdLineParse():
	'''
	Command Line Parser.
	'''
	parser = argparse.ArgumentParser(description='Run topsApp.py in parallel with multiple cores/CPUs/processors using MPI4PY')
	parser.add_argument('-slc', '--slc', type=str, required=True, help='Path to SLC files', dest='slc')
	parser.add_argument('-orb', '--orb', type=str, required=True, help='Path to orbit files', dest='orb')
	parser.add_argument('-aux', '--aux', type=str, required=True, help='Path to auxiliary files', dest='aux')
	parser.add_argument('-dem', '--dem', type=str, required=True, help='Name of dem file in .dem format', dest='dem')
	parser.add_argument('-s', '--start', default = None, type=str, required=True, help='Start date', dest='start')
	parser.add_argument('-e', '--end'  , default = None, type=str, required=True, help='End date', dest='end')
	inputs = parser.parse_args()    
	return inputs

def manuallogger(logfile, method, string):
	with open(logfile, method) as myfile:
		crttm = (datetime.datetime.now())
		if method == "w":
			tmp_str = str(crttm) + " | INFO: " + string
			myfile.write(tmp_str)
		elif method == "a":
			tmp_str = "\r\n" + str(crttm) + " | INFO: " + string
			myfile.write(tmp_str)		


if __name__ == '__main__':
	print
	print ("##########################################################################################################################")
	print ("#                                                                                                                        #")
	print ("#              This is a tool used to run topsApp.py from 'startup' to 'computeBaselines' for choosing IFGs              #")
	print ("#                                                                                                                        #")
	print ("##########################################################################################################################")
	
	inputs    = cmdLineParse()
	slcdir    = os.path.abspath(inputs.slc)
	orbdir    = os.path.abspath(inputs.orb)
	auxdir    = os.path.abspath(inputs.aux)
	demfle    = os.path.abspath(inputs.dem)
	startdate = inputs.start
	enddate   = inputs.end
	xmlfile   = 'topsApp.xml'
	cwd = os.getcwd()
	
	errstop = False
	if not os.path.isfile(xmlfile):
		print ('The xml file: %s is NOT existent.' %(os.path.join(cwd, xmlfile)))
		errstop = True		
	
	demlst = [demfle, demfle+'.vrt', demfle+'.wgs84', demfle+'.wgs84.vrt', demfle+'.wgs84.xml', demfle+'.xml']	
	for elem in demlst:
		if not os.path.isfile(elem):
			print('The dem file: %s is NOT existent.' %(os.path.join(cwd, elem)))
			errstop = True
	del demlst
	dempth, demfle = os.path.split(demfle)

	if errstop:
		sys.exit()

	for root, dirs, files in os.walk(slcdir):
		if root == slcdir:	# this is to collect all files included in slcdir only. Not collect files in subdirs.			
			filelst = files
			break
	
	filelst = list(filter(lambda x: x[-4:] == '.zip', filelst))			# this is to filter out files NOT in .zip format	
		
	datelststr = list(map(lambda x: x[17:25], filelst))
	datelstord = list(map(lambda x: (date(int(x[:4]), int(x[4:6]), int(x[6:]))).toordinal(), datelststr))	# Convert to ordinal dates
	
	ziplst = [(a, b, c) for a, b, c in zip(filelst, datelststr, datelstord)]	
	ziplst.sort(key=lambda x: x[2])									# this is to sort all three lists: filelst, datelststr, datelstord by ascending of datelstord
																	# this is useful in case of processing combine S1A & S1B.

	if startdate != 'none':
		ziplst = list(filter(lambda x: x[2] >= (date(int(startdate[:4]), int(startdate[4:6]), int(startdate[6:]))).toordinal(), ziplst))
		
	if enddate != 'none':
		ziplst = list(filter(lambda x: x[2] <= (date(int(enddate[:4]), int(enddate[4:6]), int(enddate[6:]))).toordinal(), ziplst))
	
	filelst    = list(map(lambda x: x[0], ziplst))
	datelststr = list(map(lambda x: x[1], ziplst))
	datelstord = list(map(lambda x: x[2], ziplst))
	del ziplst

	sttm = datetime.datetime.now()
	logfile = os.path.splitext(os.path.basename(__file__))[0] + '_log.txt'
	logfile = os.path.join(cwd, logfile)
	
	if os.path.isfile(logfile):
		print('The log file: %s is existent. Please rename or delete before running this script' %logfile)
		sys.exit()
	
	manuallogger(logfile, "w", "Run topsApp.py to calculate perpendicular baselines for IFGs between 1st image and the others.")
	manuallogger(logfile, "a", "Prog. started at: " + str(sttm))
	manuallogger(logfile, "a", "SLC files are stored in the dir: " + slcdir)
	manuallogger(logfile, "a", "Orb files are stored in the dir: " + orbdir)
	manuallogger(logfile, "a", "AUX files are stored in the dir: " + auxdir)
	manuallogger(logfile, 'a', 'Start date: ' + startdate)
	manuallogger(logfile, 'a', 'End date  : ' + enddate)
	manuallogger(logfile, "a", "Number of SLC files between start and end dates: " + str(len(filelst)))
	startstep = "startup"
	endstep   = "computeBaselines"
	manuallogger(logfile, 'a', 'Start step: ' + startstep)
	manuallogger(logfile, 'a', 'End step  : ' + endstep)
	manuallogger(logfile, "a", "-------------------------------------------------------------------------------------------------------")
	
	runifgs = 0	
	if not os.path.isfile(xmlfile):
		print('The file %s is not existent!' % xmlfile)
		sys.exit()

	for item in range(1, len(filelst)):
		manuallogger(logfile, "a","")
		os.chdir(cwd)
		subdir = datelststr[0] + '-' + datelststr[item]
		if os.path.isdir(subdir):
			print('\n-----------------------------------------------------------------------------------')
			print('The directory: %d / %d: %s is existent. topsApp.py will not run for this IFG.' %(item, len(filelst) - 1, subdir))
			manuallogger(logfile, 'a', 'The directory: %s is existent. topsApp.py will not run for this IFG.'  % subdir)
		else:
			runifgs += 1
			print('\n-----------------------------------------------------------------------------------')
			print('Run topsApp.py for the directory: %d / %d: %s.\n' %(item, len(filelst) - 1, subdir))			
			manuallogger(logfile, 'a', 'Run topsApp.py for the directory: %d / %d: %s.' %(item, len(filelst) - 1, subdir))
			sttm1 = datetime.datetime.now()			
			
			cmd = "mkdir " + subdir			
			subprocess.call(cmd, shell=True)
			
			manuallogger(logfile, 'a', '\t\tCopy 6 dem files to IFG directory.')
			cmd = 'cp ' + os.path.join(dempth, demfle + '* ') + subdir + '/.'
			subprocess.call(cmd, shell=True)
			
			manuallogger(logfile, 'a', '\t\tCopy topsApp.xml file to IFG directory.')
			cmd = 'cp ' + xmlfile + ' ' + subdir + '/.'
			subprocess.call(cmd, shell=True)
			os.chdir(subdir)
			
			manuallogger(logfile, 'a', '\t\tEdit topsApp.xml file to fit with input data corresponding to processed IFG directory.')
			tree = ET.parse(xmlfile)
			root = tree.getroot()
			for child1 in root:				
				for child2 in child1:					
					if child2.attrib['name'] in ['master', 'slave']:
						for child3 in child2:
							if child3.attrib['name'] == 'orbit directory':
								child3.text = orbdir
							elif child3.attrib['name'] == 'auxiliary data directory':
								child3.text = auxdir
							elif child3.attrib['name'] == 'SAFE':
								if child2.attrib['name'] == 'master':
									child3.text = os.path.join(slcdir, filelst[0])
								elif child2.attrib['name'] == 'slave':
									child3.text = os.path.join(slcdir, filelst[item])
					elif child2.attrib['name'] == 'demFilename':
						child2.text = demfle + '.wgs84'
			tree.write(xmlfile)
					
			cmd = "topsApp.py " + "topsApp.xml"
			if startstep != "":
				cmd = cmd + " --start=" + "'" + startstep + "'"
			if endstep != "":
				cmd = cmd + " --end=" + "'" + endstep + "'"
				
			cmd = cmd + ' > topsApp.py_log.txt'
				
			manuallogger(logfile, 'a', '\t\tRun ISCE by: ' + cmd)
			subprocess.call(cmd, shell=True)
			
			manuallogger(logfile, 'a', '\t\tDelete 6 dem files in processed IFG for saving disk space.')
			cmd = 'rm ' + demfle + '* '
			subprocess.call(cmd, shell=True)
			
			fntm1 = datetime.datetime.now()
			manuallogger(logfile, "a","")
			manuallogger(logfile, "a",     "\t\tStarted  at : " + str(sttm1))
			manuallogger(logfile, "a",     "\t\tFinished at : " + str(fntm1))
			manuallogger(logfile, "a",     "\t\tRunning time: " + str(fntm1 - sttm1))
			
			print("\t\tStarted  at : " + str(sttm1))
			print("\t\tFinished at : " + str(fntm1))
			print("\t\tRunning time: " + str(fntm1 - sttm1))
			
	os.chdir(cwd)
	
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	fntm = datetime.datetime.now()
	manuallogger(logfile, "a", "=============================================================")
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a",     "Program started  at             : " + str(sttm))
	manuallogger(logfile, "a",     "Program finished at             : " + str(fntm))
	manuallogger(logfile, "a",     "Total running time              : " + str(fntm - sttm))
	if runifgs > 0:
		manuallogger(logfile, "a", "Average running time for one IFG: " + str((fntm - sttm)/runifgs))
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "=============================================================")

	print("-------------------------------------------------------------")
	print("=============================================================")
	print("-------------------------------------------------------------")
	print("Program started  at             : " + str(sttm))
	print("Program finished at             : " + str(fntm))
	print("Total running time              : " + str(fntm - sttm))
	if runifgs > 0:
		print("Average running time for one IFG: " + str((fntm - sttm)/runifgs))
	print("-------------------------------------------------------------")
	print("=============================================================")
