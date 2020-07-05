#!/usr/bin/env python
import argparse
import subprocess
import os, sys, glob, shutil
from datetime import date
import datetime
import xml.etree.ElementTree as ET
import multiprocessing as mp

''' This is a tool used to run topsApp.py in full in parallelisation for multiple IFGs, which ave been chosen by topsApp_inTurn_2_choose_IFG.py.
	This requires IFGs list in file: topsApp_inTurn_2_choose_IFG_IFGs.txt
	This is coded by Luyen BUI on: 30 JUNE 2020'''

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
	parser.add_argument('-n', '--ncpus', type=int, required=True, help='Number of CPUs used in parallel running', dest='cpus')
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

def iscexmlgen(xmffle,orbdir,auxdir,mstfle,slvfle,dempth,demfle):
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

def runisce(subdir):
	sttm = datetime.datetime.now()
	print('----------\nrun for: %s' %(subdir))	
	os.chdir(subdir)
	#sublogfile = os.path.splitext(os.path.basename(__file__)[0] + '_runtime.txt')	
	sublogfile = 'runtime.txt'
	manuallogger(sublogfile, "w", "\tStarted  at : " + str(sttm))

	os.system("topsApp.py topsApp.xml > topsApp_log.txt")

	cmd = 'rm -r coarse_* ESD fine_* geom_master'
	#manuallogger(sublogfile, 'a', '\tDelete multiple directories to save disk space by: %s' %cmd)
	subprocess.call(cmd, shell=True)
	fntm = datetime.datetime.now()
		
	manuallogger(sublogfile, "a", "\tFinished at : " + str(fntm))
	manuallogger(sublogfile, "a", "\tRunning time: " + str(fntm - sttm))
	return True
	# cho nay
			
def topsApp_run(cwd,logfile,totalifgno,ifgno,ifgfullpath,orbdir,auxdir,masterslc,slaveslc,dempth,demfle,xmlfile):
	os.chdir(cwd)
	#subdir = os.path.join(ifgfullpath, 'merged')
	phafile,cohfile = 'filt_topophase.unw.geo','topophase.cor.geo'
	prtline = '-----------------------------------------------------------------------------------------------------------------------------'
	if os.path.isfile(os.path.join(ifgfullpath,'merged',phafile)) and os.path.isfile(os.path.join(ifgfullpath,'merged',cohfile)):
		# This is for the case this ifgfullpath had not been existent (its flag = False)
		# but it was run by another computer, e.g., spatial01 or Geodesy02 server,
		# and copied to this current dir. while this script is running.
		print('%s\nThe directory: %d / %d: %s with %s & %s has been copied to this current directory while this script is running. topsApp.py will not run for this IFG.'  %(prtline,ifgno,totalifgno,subdir,phafile,cohfile))
		#manuallogger(logfile, "a","")		
		manuallogger(logfile, 'a', 'The directory: %d / %d: %s with %s & %s has been copied to this current directory while this script is running. topsApp.py will not run for this IFG.'  %(ifgno, totalifgno, subdir, phafile, cohfile))
		return False
	else:
		print('%s\nRun topsApp.py for the directory: %d / %d: %s.' %(prtline,ifgno,totalifgno,ifgfullpath))
		#manuallogger(logfile, "a","")
		manuallogger(logfile, "a", 'Run topsApp.py for the directory: %d / %d: %s.' %(ifgno, totalifgno, os.path.split(ifgfullpath)[1]))		
		sttm = datetime.datetime.now()
		if not os.path.isdir(ifgfullpath):
			cmd = "mkdir " + ifgfullpath
			subprocess.call(cmd, shell=True)
		sublogfile = os.path.join(ifgfullpath, os.path.splitext(os.path.basename(__file__))[0] + '_runtime.txt')
		manuallogger(sublogfile, 'w', '\tCopy 6 dem files to IFG directory.')
		cmd = 'cp ' + os.path.join(dempth, demfle + '* ') + ifgfullpath + '/.'
		subprocess.call(cmd, shell=True)
		
		manuallogger(sublogfile, 'a', '\tCopy topsApp.xml file to IFG directory.')
		cmd = 'cp ' + xmlfile + ' ' + ifgfullpath + '/.'
		subprocess.call(cmd, shell=True)
		os.chdir(ifgfullpath)		
		manuallogger(sublogfile, 'a', '\tEdit topsApp.xml file to fit with input data corresponding to processed IFG directory.')	
		iscexmlgen(xmlfile,orbdir,auxdir,masterslc,slaveslc,dempth,demfle)

		cmd = "topsApp.py " + "topsApp.xml"
		'''startstep = "startup"
		endstep   = "computeBaselines"
		if startstep != "":
			cmd = cmd + " --start=" + "'" + startstep + "'"
		if endstep != "":
			cmd = cmd + " --end=" + "'" + endstep + "'"'''
			
		cmd = cmd + ' > topsApp.py_log.txt'			
		manuallogger(sublogfile, 'a', '\tRun ISCE by: ' + cmd)
		subprocess.call(cmd, shell=True)
	
		manuallogger(sublogfile, 'a', '\tDelete 6 dem files in processed IFG for saving disk space.')
		cmd = 'rm ' + demfle + '* '
		subprocess.call(cmd, shell=True)
	
		cmd = 'rm -r coarse_* ESD fine_* geom_master'
		manuallogger(sublogfile, 'a', '\tDelete multiple directories to save disk space by: %s' %cmd)
		subprocess.call(cmd, shell=True)
	
		fntm = datetime.datetime.now()
		manuallogger(sublogfile, "a", "")
		manuallogger(sublogfile, "a", "\tStarted  at : " + str(sttm))
		manuallogger(sublogfile, "a", "\tFinished at : " + str(fntm))
		manuallogger(sublogfile, "a", "\tRunning time: " + str(fntm - sttm))
		return True
		


if __name__ == '__main__':
	print
	print ("##########################################################################################################################")
	print ("#                                                                                                                        #")
	print ("#                      This is a tool used to run topsApp.py in full for multiple IFGs in parallel                       #")
	print ("#                                                                                                                        #")
	print ("##########################################################################################################################")
	
	inputs = cmdLineParse()
	slcdir,orbdir,auxdir  = os.path.abspath(inputs.slc),os.path.abspath(inputs.orb),os.path.abspath(inputs.aux)
	ifgfle,demfle,xmlfile = os.path.abspath(inputs.ifg),os.path.abspath(inputs.dem),'topsApp.xml'
	cpus,cwd = inputs.cpus,os.getcwd()
	
	'''errstop = False
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
	slcdatelst = list(map(lambda x: x[17:25], slcfilelst))						# Extract dates in string format of all files in the list
	
	ifglstall = ifgread(ifgfle)	
	chklst = [item for sublist in ifglstall for item in sublist]				# Collect images from ifg list
	chklst = list(dict.fromkeys(chklst))										# Remove duplicate items
	
	errstop = False
	for item in chklst:
		if item not in slcdatelst:
			print('The image: %s is NOT available in SLC directory: %s' %(item, slcdir))
			errstop = True		
	del chklst	
	if errstop == True:
		sys.exit()
	
	sttm = datetime.datetime.now()
	prevlogslist = glob.glob(os.path.splitext(os.path.basename(__file__))[0] + '_log_*.txt')
	test = len(prevlogslist)
	if len(prevlogslist) > 0:
		nextlogsno = str(max([int(item[-6:-4]) for item in prevlogslist])+1)
	else:
		nextlogsno = str(1)
	logfile = os.path.splitext(os.path.basename(__file__))[0] + '_log_' + nextlogsno.zfill(2) + '.txt'
	logfile = os.path.join(cwd, logfile)	
	if os.path.isfile(logfile):
		print('The log file: %s is existent. Please rename or delete before running this script' %logfile)
		sys.exit()
	
	manuallogger(logfile, "w", "run topsApp.py in full for multiple IFGs in parallel.")
	manuallogger(logfile, "a", "Number of CPUs/COREs, which s used: " + str(cpus))
	manuallogger(logfile, "a", "Prog. started at: " + str(sttm))
	manuallogger(logfile, "a", "SLC files are stored in the dir: " + slcdir)
	manuallogger(logfile, "a", "Orb files are stored in the dir: " + orbdir)
	manuallogger(logfile, "a", "AUX files are stored in the dir: " + auxdir)
	manuallogger(logfile, "a", "DEM files, which will be copied and used: " + os.path.join(dempth,demfle+'*'))
	manuallogger(logfile, "a", "xml file, which will be copied, edit and used: " + os.path.join(cwd,xmlfile))
	manuallogger(logfile, "a", "List of IFGs, which will be processed is in: " + ifgfle)
	manuallogger(logfile, "a", "Number of IFGs, which will be processed: " + str(len(ifglstall)))	
	manuallogger(logfile, "a", "-------------------------------------------------------------------------------------------------------")
	
	ifgflag,ifglstrun = [False] * len(ifglstall),[]
	for idx, item in enumerate(ifglstall):
		os.chdir(cwd)
		subdir = item[0] + '-' + item[1]
		phafile,cohfile = 'filt_topophase.unw.geo','topophase.cor.geo'
		prtline = '-----------------------------------------------------------------------------------------------------------------------------'
		if os.path.isfile(os.path.join(subdir,'merged',phafile)) and os.path.isfile(os.path.join(subdir,'merged',cohfile)):
			print('%s\nThe directory: %s is existent with %s & %s included. topsApp.py will not run for this IFG.' %(prtline,subdir,phafile,cohfile))
			manuallogger(logfile, 'a', 'The directory: %s is existent with %s & %s included. topsApp.py will not run for this IFG.' %(subdir,phafile,cohfile))
			ifgflag[idx] = True
		else:
			ifglstrun.append(item)
			if os.path.isdir(subdir):
				print('%s\nDelete the directory: %s, which is existent but %s & %s are not included.' %(prtline,subdir,phafile,cohfile))
				manuallogger(logfile, 'a', 'Delete the directory: %s, which is existent but %s & %s are not included.' %(subdir,phafile,cohfile))
				shutil.rmtree(subdir)
				#cmd = "rm -r " + subdir
				#subprocess.call(cmd, shell=True)
	
	ifgdirrun = [os.path.join(cwd, item[0]+'-'+item[1]) for item in ifglstrun]
	mpinargs = [(cwd,logfile,len(ifgdirrun),id+1,ifgdirrun[id],orbdir,auxdir,slcfilelst[slcdatelst.index(ifglstrun[id][0])],slcfilelst[slcdatelst.index(ifglstrun[id][1])],dempth,demfle,xmlfile) for id in range(len(ifgdirrun))]
	sttm2 = datetime.datetime.now()
	pool = mp.Pool(cpus)
	results = pool.starmap(topsApp_run, mpinargs)
	pool.close'''

	sttm = datetime.datetime.now()

	ifglstall = ifgread(ifgfle)
	subdirs = [item[0]+'-'+item[1] for item in ifglstall]

	'''pool = mp.Pool(2)
	results = pool.map(runisce,subdirs)'''

	processes = []
	for subdir in subdirs:
		p = mp.Process(target=runisce, args=(subdir,))
		processes.append(p)
		p.start()
		
	for process in processes:
		process.join()

	fntm = datetime.datetime.now()	
	#os.chdir(cwd)	
	'''manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "=============================================================")
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "Program started  at           : " + str(sttm))
	manuallogger(logfile, "a", "Program finished at           : " + str(fntm))
	manuallogger(logfile, "a", "Total running time [all steps]: " + str(fntm - sttm))
	if sum(results) > 0:
		manuallogger(logfile, "a", "Average running time [topsapp.py only] for one IFG: " + str((fntm - sttm2)/sum(results)))
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "=============================================================")'''

	print("-------------------------------------------------------------")
	print("-------------------------------------------------------------")
	print("=============================================================")
	print("-------------------------------------------------------------")
	print("Program started  at           : " + str(sttm))
	print("Program finished at           : " + str(fntm))
	print("Total running time [all steps]: " + str(fntm - sttm))
	print("Average running time [topsapp.py only] for one IFG: " + str((fntm - sttm)/2))
	#if sum(results) > 0:
		#print("Average running time [topsapp.py only] for one IFG: " + str((fntm - sttm2)/sum(results)))
	print("-------------------------------------------------------------")
	print("=============================================================")
