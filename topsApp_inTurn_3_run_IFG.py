#!/usr/bin/env python
import argparse
import subprocess
import os, sys, glob, shutil
from datetime import date
import datetime
import xml.etree.ElementTree as ET

''' This is a tool used to run topsApp.py in full in sequence for multiple IFGs that have been chosen by topsApp_inTurn_2_choose_IFG.py.
	This requires IFGs list in file: topsApp_inTurn_2_choose_IFG_IFGs.txt
	This is coded by Luyen BUI on: 06 JUNE 2019'''

def cmdLineParse():
	'''
	Command Line Parser.
	'''
	parser = argparse.ArgumentParser(description='Run topsApp.py in parallel with multiple cores/CPUs/processors using MPI4PY')
	parser.add_argument('-slc', '--slc', type=str, required=True, help='Path to SLC files', dest='slc')
	parser.add_argument('-orb', '--orb', type=str, required=True, help='Path to orbit files', dest='orb')
	parser.add_argument('-aux', '--aux', type=str, required=True, help='Path to auxiliary files', dest='aux')
	parser.add_argument('-dem', '--dem', type=str, required=True, help='Name of dem file in .dem format', dest='dem')
	parser.add_argument('-ifg', '--ifg', type=str, required=True, help='Name of txt file of which IFGs list is included', dest='ifg')
	inputs = parser.parse_args()    
	return inputs
	
def ifgread(ifglstfile):
	ifglst = []	
	rfile = open(ifglstfile, "r")	
	cnt = rfile.readlines()	
	rfile.close()	
	for line in cnt:
		ifglst.append([line.split()[0], line.split()[1]])		
	return ifglst
	
def manuallogger(logfile, method, string):
	with open(logfile, method) as myfile:
		crttm = (datetime.datetime.now())
		if method == "w":
			tmp_str = str(crttm) + " | INFO: " + string
			myfile.write(tmp_str)
		elif method == "a":
			tmp_str = "\r\n" + str(crttm) + " | INFO: " + string
			myfile.write(tmp_str)

def iscexmlgen(xmffle,orbdir,auxdir,mstfle,slvfle,demfle):
	tree = ET.parse(xmffle)
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
							child3.text = os.path.join(slcdir, mstfle)
						elif child2.attrib['name'] == 'slave':
							child3.text = os.path.join(slcdir, slvfle)
			elif child2.attrib['name'] == 'demFilename':
				child2.text = demfle + '.wgs84'
	tree.write(xmffle)		



if __name__ == '__main__':
	sttm_all = datetime.datetime.now()
	print
	print ("##########################################################################################################################")
	print ("#                                                                                                                        #")
	print ("#                      This is a tool used to run topsApp.py in full for multiple IFGs in sequence                       #")
	print ("#                                                                                                                        #")
	print ("##########################################################################################################################")
	
	inputs = cmdLineParse()
	slcdir,orbdir,auxdir  = os.path.abspath(inputs.slc),os.path.abspath(inputs.orb),os.path.abspath(inputs.aux)
	ifgfle,demfle,xmlfile = os.path.abspath(inputs.ifg),os.path.abspath(inputs.dem),'topsApp.xml'
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
	dempth,demfle = os.path.split(demfle)

	if errstop:
		sys.exit()
	
	for root, dirs, files in os.walk(slcdir):
		if root == slcdir:	# this is to collect all files included in slcdir only. Not collect files in subdirs.			
			slcfilelst = files
			break
	
	slcfilelst = list(filter(lambda x: x[-4:] == '.zip', slcfilelst))			# this is to filter out files NOT in .zip format	
	slcdatelst = list(map(lambda x: x[17:25], slcfilelst))					# Extract dates in string format of all files in the list
	
	ifglst = ifgread(ifgfle)
	ifgs   = len(ifglst)
	
	chklst = [item for sublist in ifglst for item in sublist]				# Collect images from ifg list
	chklst = list(dict.fromkeys(chklst))							# Remove duplicate items
	
	errstop = False
	for item in chklst:
		if item not in slcdatelst:
			print('The image: %s is NOT available in SLC directory: %s' %(item, slcdir))
			errstop = True		
	del chklst	
	if errstop == True:
		sys.exit()
	
	prevlogslist = glob.glob(os.path.splitext(os.path.basename(__file__))[0] + '_log_*.txt')
	if len(prevlogslist) > 0:
		nextlogsno   = str(max([int(item[-6:-4]) for item in prevlogslist])+1)
	else:
		nextlogsno = str(1)
	logfile = os.path.splitext(os.path.basename(__file__))[0] + '_log_' + nextlogsno.zfill(2) + '.txt'
	logfile = os.path.join(cwd,logfile)	
	if os.path.isfile(logfile):
		print('The log file: %s is existent. Please rename or delete before running this script' %logfile)
		sys.exit()
	
	manuallogger(logfile, "w", "Run topsApp.py in full for multiple IFGs in sequence.")
	manuallogger(logfile, "a", "Prog. started at: " + str(sttm_all))
	manuallogger(logfile, "a", "SLC files are stored in the dir            : " + slcdir)
	manuallogger(logfile, "a", "Orb files are stored in the dir            : " + orbdir)
	manuallogger(logfile, "a", "AUX files are stored in the dir            : " + auxdir)
	manuallogger(logfile, "a", "DEM files that will be copied and used     : " + os.path.join(dempth,demfle + '*'))
	manuallogger(logfile, "a", "xml file that will be copied, edit and used: " + os.path.join(cwd,xmlfile))
	manuallogger(logfile, "a", "List of IFGs that will be processed is in  : " + ifgfle)
	manuallogger(logfile, "a", "Number of IFGs that will be considered: " + str(ifgs))	
	manuallogger(logfile, "a", "--------------------------------------------------------------------------------------------------------------------------------------------")
	
	runifgs = len(ifglst)
	ifgflag = [False] * runifgs
	os.chdir(cwd)
	prtline = '-----------------------------------------------------------------------------------------------------------------------------'
	phafile,cohfile = 'filt_topophase.unw.geo','topophase.cor.geo'
	for idx, item in enumerate(ifglst):		
		subdir = item[0] + '-' + item[1]
		if os.path.isfile(os.path.join(subdir,'merged',phafile)) and os.path.isfile(os.path.join(subdir,'merged',cohfile)):
			print('%s\nThe directory: %s is existent with %s & %s included. topsApp.py will not run for this IFG.' %(prtline,subdir,phafile,cohfile))
			manuallogger(logfile, 'a', 'The directory: %s is existent with %s & %s included. topsApp.py will not run for this IFG.' %(subdir,phafile,cohfile))
			runifgs -= 1
			ifgflag[idx] = True
	
	for item in ifglst:
		subdir = item[0] + '-' + item[1]
		if os.path.isdir(subdir) and (not os.path.isfile(os.path.join(subdir,'merged',phafile)) or not os.path.isfile(os.path.join(subdir,'merged',cohfile))):
			print('%s\nDelete the directory: %s, which is existent but %s or %s are not included.' %(prtline,subdir,phafile,cohfile))
			manuallogger(logfile, 'a', 'Delete the directory: %s, which is existent but %s or %s are not included.' %(subdir,phafile,cohfile))
			shutil.rmtree(subdir) # This does not work in Ubuntu shared folder. Use below.
			#cmd = "rm -r " + subdir
			#subprocess.call(cmd, shell=True)

	ifgno, count, sttm_tops = 0, 0, datetime.datetime.now()
	for idx, item in enumerate(ifglst):		
		if not ifgflag[idx]:
			#os.chdir(cwd)
			ifgno += 1			
			subdir = item[0] + '-' + item[1]
			if os.path.isfile(os.path.join(subdir,'merge',phafile)) and os.path.isfile(os.path.join(subdir,'merge',cohfile)):
				# This is for the case this subdir had not been existent before this script was called (its flag = False)
				# but it was run by another computer, e.g., spatial01 or Geodesy02 server,
				# and copied to this current dir. while this script is running.
				print('%s\nThe directory: %d / %d: %s has been copied to this current directory while this script is running. topsApp.py will not run for this IFG.'  %(prtline,ifgno,runifgs,subdir))
				manuallogger(logfile, 'a', 'The directory: %d / %d: %s has been copied to this current directory while this script is running. topsApp.py will not run for this IFG.'  %(ifgno, runifgs, subdir))
			else:			
				count += 1
				print('%s\nRun topsApp.py for the directory: %d / %d: %s.' %(prtline,ifgno,runifgs,subdir))
				manuallogger(logfile, 'a', 'Run topsApp.py for the directory: %d / %d: %s.' %(ifgno,runifgs,subdir))
				sttm_subdir = datetime.datetime.now()
				
				if not os.path.isdir(subdir):
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
				iscexmlgen(xmlfile,orbdir,auxdir,os.path.join(slcdir,slcfilelst[slcdatelst.index(item[0])]),os.path.join(slcdir,slcfilelst[slcdatelst.index(item[1])]),demfle)
			
				cmd = "topsApp.py " + "topsApp.xml"
				'''startstep = "startup"
				endstep   = "computeBaselines"
				if startstep != "":
					cmd = cmd + " --start=" + "'" + startstep + "'"
				if endstep != "":
					cmd = cmd + " --end=" + "'" + endstep + "'"'''
					
				cmd = cmd + ' > topsApp.py_log.txt'
					
				manuallogger(logfile, 'a', '\t\tRun ISCE by: ' + cmd)
				subprocess.call(cmd, shell=True)
			
				manuallogger(logfile, 'a', '\t\tDelete 6 dem files in processed IFG for saving disk space.')
				cmd = 'rm ' + demfle + '* '
				subprocess.call(cmd, shell=True)
			
				cmd = 'rm -r coarse_* ESD fine_* geom_master'
				manuallogger(logfile, 'a', '\t\tDelete multiple directories to save disk space by: %s' %cmd)
				subprocess.call(cmd, shell=True)

				os.chdir(cwd)
				if not os.path.isfile(os.path.join(subdir,'merge',phafile)) or not os.path.isfile(os.path.join(subdir,'merge',cohfile)):
					# This is for the case topsApp.py did not run successfully 
					# that 'filt_topophase.unw.geo' &/or 'topophase.cor.geo' were not generated
					# usually coherence threshold is too high that no point left to subsequently used
					print('\t\tDelete current directory as topsApp.py has not run successfully')
					manuallogger(logfile, 'a', '\t\tDelete current directory as topsApp.py has not run successfully')
					shutil.rmtree(subdir) # This does not work with Ubuntu shared folder. Use below.
					#cmd = "rm -r " + subdir
					#subprocess.call(cmd, shell=True)
			
				fntm_subdir = datetime.datetime.now()
				manuallogger(logfile, "a","")
				manuallogger(logfile, "a", "\t\tStarted  at : " + str(sttm_subdir))
				manuallogger(logfile, "a", "\t\tFinished at : " + str(fntm_subdir))
				manuallogger(logfile, "a", "\t\tRunning time: " + str(fntm_subdir - sttm_subdir))
			
				print("\t\tStarted  at : " + str(sttm_subdir))
				print("\t\tFinished at : " + str(fntm_subdir))
				print("\t\tRunning time: " + str(fntm_subdir - sttm_subdir))

	fntm_all = datetime.datetime.now()
	#os.chdir(cwd)
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "=============================================================")
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "Program started  at           : " + str(sttm_all))
	manuallogger(logfile, "a", "Program finished at           : " + str(fntm_all))
	manuallogger(logfile, "a", "Total running time [all steps]: " + str(fntm_all - sttm_all))
	if count > 0:
		manuallogger(logfile, "a", "Average running time [topsapp.py only] for one IFG: " + str((fntm_all - sttm_tops)/count))
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "=============================================================")

	print("-------------------------------------------------------------")
	print("-------------------------------------------------------------")
	print("=============================================================")
	print("-------------------------------------------------------------")
	print("Program started  at           : " + str(sttm_all))
	print("Program finished at           : " + str(fntm_all))
	print("Total running time [all steps]: " + str(fntm_all - sttm_all))
	if count > 0:
		print("Average running time [topsapp.py only] for one IFG: " + str((fntm_all - sttm_tops)/count))
	print("-------------------------------------------------------------")
	print("=============================================================")
