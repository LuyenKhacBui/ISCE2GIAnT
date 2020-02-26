#!/usr/bin/env python3

import datetime
from datetime import date
import os, sys
import argparse
import numpy as np
import matplotlib.pyplot as plt

from os.path import dirname as dirname
from os.path import abspath as abspath

appdir = dirname(dirname(abspath(__file__)))
##sys.path.append(appdir)
sys.path.insert(0, appdir)

from modules.geotools import DecimalYearComp as decyrcomp

''' This is a tool used to read ISCE results from topsApp_inTurn_1_prep_IFG.py then choose SBAS IFGs based on temporal and perpendicular baseline thresholds'
	This is coded by Luyen BUI on: 06 JUNE 2019'''

def cmdLineParse():
	'''
	Command Line Parser.
	'''
	parser = argparse.ArgumentParser(description="Read ISCE's results to choose SBAS-based IFGs based on temporal and perpendicular baseline thresholds")
	parser.add_argument('-bt','--btemp', type=int, required=True, help="Temporal baseline threshold in days.", dest='btemp')
	parser.add_argument('-bp','--bperp', type=int, required=True, help="Perpendicular baseline threshold in meters.", dest='bperp')
	inputs = parser.parse_args()	
	return inputs
	
def bperpread(iscelogfile):
	bperp_iw = np.full(6, np.nan, dtype=float)
	
	rfile = open(iscelogfile, "r")	
	cnt = rfile.readlines()	
	rfile.close()
				
	for line in cnt:
		if line[:57] == 'baseline.IW-1 Bperp at midrange for first common burst = ':
			bperp_iw[0] = float(line[57:])
		elif line[:57] == 'baseline.IW-2 Bperp at midrange for first common burst = ':
			bperp_iw[2] = float(line[57:])
		elif line[:57] == 'baseline.IW-3 Bperp at midrange for first common burst = ':
			bperp_iw[4] = float(line[57:])
			
		if line[:56] == 'baseline.IW-1 Bperp at midrange for last common burst = ':
			bperp_iw[1] = float(line[56:])
		elif line[:56] == 'baseline.IW-2 Bperp at midrange for last common burst = ':
			bperp_iw[3] = float(line[56:])
		elif line[:56] == 'baseline.IW-3 Bperp at midrange for last common burst = ':
			bperp_iw[5] = float(line[56:])
	
	return np.nanmean(bperp_iw)
	
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
	print ("#  This is a tool used to read perpendicular baselines from IFG sub-direcrories then choose IFGs based on SBAS approach  #")
	print ("#                                                                                                                        #")
	print ("##########################################################################################################################")
	
	inputs = cmdLineParse()
	btmpth = inputs.btemp
	bprpth = inputs.bperp
	
	sttm = datetime.datetime.now()
	cwd = os.getcwd()
	ifgdir = sorted([x[1] for x in os.walk(cwd)][0])
	logfile = os.path.splitext(os.path.basename(__file__))[0] + '_log.txt'
	logfile = os.path.join(cwd, logfile)	
	outfile = os.path.splitext(os.path.basename(__file__))[0] + '_IFGs.txt'
	outfile = os.path.join(cwd, outfile)
	pngfile = os.path.splitext(os.path.basename(__file__))[0] + '_IFGs.png'
	pngfile = os.path.join(cwd, pngfile)
	
	if os.path.isfile(logfile):
		print('The log file: %s is existent. Please rename or delete before running this script' %logfile)
		sys.exit()
	if os.path.isfile(outfile):
		print('The out file: %s is existent. Please rename or delete before running this script' %outfile)
		sys.exit()
	
	manuallogger(logfile, "w", "Read perpendicular baselines from sub-direcrories then choose SBAS IFGs based on temporal and perpendicular baseline thresholds.")
	manuallogger(logfile, "a", "Prog. started at: " + str(sttm))
	manuallogger(logfile, "a", "IFGs sub-direcrories located in: " + cwd)	
	manuallogger(logfile, "a", "Number of IFGs sub-direcrories : " + str(len(ifgdir)))
	manuallogger(logfile, "a", "Temporal baseline threshold [days]: " + str(btmpth))
	manuallogger(logfile, "a", "Perpendicular baseline threshold [m]: " + str(bprpth))	
	manuallogger(logfile, "a", "-------------------------------------------------------------------------------------------------------")
	
	relIFGs = [[ifgdir[0][:8], 0]]
	for idir in ifgdir:
		print('Read perpendicular baseline from sub-direcrory: %s' %idir)
		manuallogger(logfile, 'a', 'Read perpendicular baseline from sub-direcrory: ' + idir)
		
		iscelogfile = os.path.join(cwd, idir, "isce.log")		
		if not os.path.isfile(iscelogfile):
			print ('\tThe file isce.log is NOT existent.')
			manuallogger(logfile, 'a', '\tThe file isce.log is NOT existent.')
			sys.exit()
		
		relIFGs.append([idir[-8:], bperpread(iscelogfile)])	
	relIFGs.sort(key=lambda x: x[0])
	
	manuallogger(logfile, "a", "-------------------------------------------------------------------------------------------------------")
	print("-------------------------------------------------------------------------------------------------------")
	print('Choose IFGs based on temporal and perpendicular baseline thresholds.')
	manuallogger(logfile, 'a', 'Choose IFGs based on temporal and perpendicular baseline thresholds.')
	IFGS = []
	for ii in range(len(relIFGs) - 1):
		for jj in range(ii + 1, len(relIFGs)):
			btemp = (date(int(relIFGs[jj][0][:4]), int(relIFGs[jj][0][4:6]), int(relIFGs[jj][0][6:]))).toordinal() - (date(int(relIFGs[ii][0][:4]), int(relIFGs[ii][0][4:6]), int(relIFGs[ii][0][6:]))).toordinal()
			bperp = relIFGs[jj][1] - relIFGs[ii][1]
			
			if btemp <= btmpth and abs(bperp) <= bprpth:
				IFGS.append([relIFGs[ii][0], relIFGs[jj][0], btemp, bperp])
				
	print('\tNumber of chosen IFGs: %d' %len(IFGS))
	manuallogger(logfile, 'a', '\tNumber of chosen IFGs: %d' %len(IFGS))
				
	open (outfile, 'w')
	with open (outfile, 'a') as myfile:
		for item in IFGS:
			print(item)
			txt = item[0] + '\t' + item[1] + '\t' + str(item[2]) + '\t' + str(item[3]) + '\r\n'
			myfile.writelines(txt)
			
	print('Chosen IFGs list has been saved to %s.' %outfile)
	print('This file can be used for running topsApp.py in full by topsApp_inTurn_3_run_IFG.py')
	manuallogger(logfile, 'a', 'Chosen IFGs list has been saved to ' + outfile)
	manuallogger(logfile, 'a', 'This file can be used for running topsApp.py in full by topsApp_inTurn_3_run_IFG.py')
	
	# From below is to plot ifgs network
	imgyear = []
	[imgyear.append(decyrcomp(int((relIFGs[scene_index][0])[0:4]), int((relIFGs[scene_index][0])[4:6]), int((relIFGs[scene_index][0])[6:8]))) for scene_index in range(len(relIFGs))]
	imgname = [x[0] for x in relIFGs]
	imgperp = [x[1] for x in relIFGs]
	
	x1 = []	
	y1 = []
	x2 = []
	y2 = []
	for ifgidx in range(len(IFGS)):
		x1.append(imgyear[imgname.index(IFGS[ifgidx][0])])
		y1.append(imgperp[imgname.index(IFGS[ifgidx][0])])
		x2.append(imgyear[imgname.index(IFGS[ifgidx][1])])
		y2.append(imgperp[imgname.index(IFGS[ifgidx][1])])
		
	#for imgidx in range(len(relIFGs)):
	#	plt.text(imgyear[imgidx], imgperp[imgidx], imgname[imgidx], horizontalalignment = 'center', verticalalignment = 'top')
		
	for ifgidx in range(len(IFGS)):
		plt.plot([x1[ifgidx], x2[ifgidx]], [y1[ifgidx], y2[ifgidx]], '-k')
		#plt.text(np.mean([x1[ifgidx], x2[ifgidx]]), np.mean([y1[ifgidx], y2[ifgidx]]), '(' + str(ifgidx + 1) + ')', horizontalalignment = 'center', verticalalignment = 'center')	
	
	plt.plot(imgyear, imgperp, 'ro')
	
	plt.axis([min(min(x1), min(x2)) - (max(max(x1), max(x2)) - min(min(x1), min(x2)))/10, max(max(x1), max(x2)) + (max(max(x1), max(x2)) - min(min(x1), min(x2)))/10, min(min(y1), min(y2)) - (max(max(y1), max(y2)) - min(min(y1), min(y2)))/10, max(max(y1), max(y2)) + (max(max(y1), max(y2)) - min(min(y1), min(y2)))/10])		
	plt.xlabel('Temporal Baseline [year]', fontsize = 14, color = 'r')
	plt.ylabel('Perpendicular Baseline [m]', fontsize = 14, color = 'b')
	plt.title('Inteferogram selection for SBAS method', fontsize = 20, color = 'r')
	plt.grid()	
	figdpi = 300
	plt.draw()	
	plt.savefig(pngfile, dpi = figdpi)
	print('IFGs network has been saved to %s.' %pngfile)
	manuallogger(logfile, 'a', 'IFGs network has been saved to %s.' %pngfile)
	
	fntm = datetime.datetime.now()
	manuallogger(logfile, "a", "=============================================================")
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "Program started  at : " + str(sttm))
	manuallogger(logfile, "a", "Program finished at : " + str(fntm))	
	manuallogger(logfile, "a", "Total running time  : " + str(fntm - sttm))
	manuallogger(logfile, "a", "-------------------------------------------------------------")
	manuallogger(logfile, "a", "=============================================================")
	
	print("=============================================================")
	print("-------------------------------------------------------------")
	print("Program started  at : " + str(sttm))
	print("Program finished at : " + str(fntm))
	print("Total running time  : " + str(fntm - sttm))
	print("-------------------------------------------------------------")
	print("=============================================================")	
