#!/usr/bin/env python3

import datetime
import argparse
import os, sys
import numpy as np
import xml.etree.ElementTree as ET

# Below is to run in my Ubuntu
sys.path.insert(0, '/home/luyenbui/tools/ISCE/install/201807/isce/components')

# Below is to run in Spatial01 server with 201807 version of ISCE
#sys.path.insert(0, '/opt/isce/install/201807/isce/components')

from iscesys.Component.ProductManager import ProductManager as PM
pm = PM()
pm.configure()



''' This is a tool used to read ISCE products then prepare ifg.list and example.rsc files used for GIAnT running
This is coded by Luyen BUI on: 07 APR 2019'''

def cmdLineParse():
	'''
	Command Line Parser.
	'''
	parser = argparse.ArgumentParser(description="Read ISCE's results to prepare ifg.list and example.rsc files for GIAnT running")
	parser.add_argument('-p','--iscepath', type=str, required=False, help="A path of ISCE' results", dest='ipath')
	
	inputs = parser.parse_args()
	if (not inputs.ipath):
		inputs.ipath = os.getcwd()		
	return inputs
	

def bperpread(logfile):
	bperp_iw = np.full(6, np.nan, dtype=float)
	
	rfile = open(logfile, "r")	
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
	
	
	
def rpacrsc(rfile):	
	'''tree = ET.parse(rfile)
	root = tree.getroot()
	
	for elem in root.iter('property'):
		if elem.attrib == {'name': 'radarwavelength'}:			
			wlen = elem.find('value').text			
			break
	
	for elem in root.iter('property'):
		if elem.attrib == {'name': 'burststartutc'}:
			tmid = elem.find('value').text
			utc = (tmid.split()[-1]).split(':')			
			utc = round(float(utc[0]) * 3600.0 + float(utc[1]) * 60.0 + float(utc[2]))
			break
			
	for elem in root.iter('property'):
		if elem.attrib == {'name': 'orbit_source'}:
			orbit = elem.find('value').text
			break'''
	
	try:
		frame = pm.loadProduct(rfile)	
		burst = frame.bursts[0]	# Obtain the first burst in the frame
		orbit = burst.orbit
		tmid  = frame.sensingMid
		utc   = 3600 * tmid.hour + 60 * tmid.minute + tmid.second  # UTC time
		wvlen = burst.radarWavelength # Wave length
		hdg   = orbit.getHeading(tmid) # heading angle
		return wvlen, hdg, utc
	except:
		return np.nan, np.nan, np.nan



def widlenread(filtfile):
	tree = ET.parse(filtfile)
	root = tree.getroot()
	for ii, child in enumerate(root):
		if child.attrib == {'name': 'width'}:						
			imwid = child.find('value').text
			#imwid = int(root[ii][0].text)
		if child.attrib == {'name': 'length'}:						
			imlen = child.find('value').text
			#imlen = int(root[ii][0].text)
	
	return imwid, imlen



if __name__ == '__main__':
	print
	print ("##########################################################################################################################")
	print ("#                                                                                                                        #")
	print ("#    This is the tool used to read ISCE's products to prepare the files ifg.list and example.rsc for GIAnT running       #")
	print ("#                                                                                                                        #")
	print ("##########################################################################################################################")

	sttm = datetime.datetime.now()
	inputs = cmdLineParse()
	ipath = os.path.abspath(inputs.ipath)
			
	ifg_dir = sorted([x[1] for x in os.walk(ipath)][0])
	ifgs = len(ifg_dir)
	
	ifg_st  = ['UNKNOWN'] * ifgs
	ifg_fn  = ['UNKNOWN'] * ifgs
	ifg_sat = ['SENT-1A'] * ifgs
	bperp = [np.nan] * ifgs
	
	imwid = [np.nan] * ifgs
	imlen = [np.nan] * ifgs
	wvlen = [np.nan] * ifgs
	hdg   = [np.nan] * ifgs
	utc   = [np.nan] * ifgs	
	
	for idir in range(ifgs):
		os.chdir(ipath + '/' + ifg_dir[idir])		
	
		if not os.path.isfile("isce.log"):
			print ('In the directory: ', ifg_dir[idir], ', the file isce.log is NOT available.')
			sys.exit()
		#else:			
		#	print ('read perp bsln for ifg: ', os.getcwd())		
					
		ifg_st[idir] = ifg_dir[idir][:8]
		ifg_fn[idir] = ifg_dir[idir][-8:]
		bperp[idir]  = bperpread('isce.log')
		
		mergdir = os.getcwd() + '/' + 'merged'
		filtfile = None
		if os.path.isdir(mergdir) and os.path.isfile(mergdir + '/' + 'filt_topophase.unw.geo.xml'):
			filtfile = mergdir + '/' + 'filt_topophase.unw.geo.xml'
			imwid[idir], imlen[idir] = widlenread(filtfile)
					
		mstdir = os.getcwd() + '/' + 'master'
		iwfile = None
		if os.path.isdir(mstdir):			
			if os.path.isfile(mstdir + '/' + 'IW1.xml'):
				iwfile = mstdir + '/' + 'IW1.xml'
			elif os.path.isfile(mstdir + '/' + 'IW2.xml'):
				iwfile = mstdir + '/' + 'IW2.xml'
			elif os.path.isfile(mstdir + '/' + 'IW3.xml'):
				iwfile = mstdir + '/' + 'IW3.xml'
				
			if iwfile != None:				
				wvlen[idir], hdg[idir], utc[idir] = rpacrsc(iwfile)				
				
		if filtfile != None and iwfile != None:
			rscfile = os.getcwd() + '/' + 'example.rsc'
			with open (rscfile, 'w') as myfile:
				myfile.writelines('WIDTH' + '\t\t\t\t\t\t\t\t' + str(imwid[idir]) + '\r\n')
				myfile.writelines('FILE_LENGTH' + '\t\t\t\t\t\t' + str(imlen[idir]) + '\r\n')
				myfile.writelines('WAVELENGTH' + '\t\t\t\t\t\t\t' + str(wvlen[idir]) + '\r\n')
				myfile.writelines('HEADING_DEG' + '\t\t\t\t\t\t' + str(hdg[idir]) + '\r\n')
				myfile.writelines('CENTER_LINE_UTC' + '\t\t\t\t\t\t' + str(utc[idir]) + '\r\n')
				
			print ('IFG: ', ifg_dir[idir], '\t', rscfile)
		else:
			print ('IFG: ', ifg_dir[idir], '\t NO example.rsc file')
						
		os.chdir(ipath)
		
	ifgfile = ipath + '/' + 'ifg.list'	
	with open ('ifg.list', 'w') as myfile:
		for ii in range(len(bperp)):
			txt = ifg_st[ii] + '\t' + ifg_fn[ii] + '\t' + ('%0.3f' % bperp[ii]) + '\t' + ifg_sat[ii] + '\r\n'
			myfile.writelines(txt)
			
	print ('\r\nInterferogram list has been saved to file: ', ifgfile)	
	print ('\r\nMax vs min perp bsln    : ', str(max(bperp)), ' vs ', str(min(bperp)))	
	print ('\r\nMax vs min image width  : ', str(max(imwid)), ' vs ', str(min(imwid)))
	print ('\r\nMax vs min image length : ', str(max(imlen)), ' vs ', str(min(imlen)))	
	print ('\r\nMax vs min heading      : ', str(np.nanmax(hdg)), ' vs ', str(np.nanmin(hdg)))
	print ('\r\nMax vs min UTC          : ', str(np.nanmax(utc)), ' vs ', str(np.nanmin(utc)))
	
	fntm = datetime.datetime.now()

print ('\r\n==========================================================================================================================')
print ('--------------------------------------------------------------------------------------------------------------------------')
print ('Prog. started at  : ' + str(sttm))
print ('Prog. finished at : ' + str(fntm))
print ('total running time: ' + str(fntm - sttm))
print ('Program finished !')
print ('--------------------------------------------------------------------------------------------------------------------------')
print ('==========================================================================================================================')
