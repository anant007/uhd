/**
 * SDR packet header
 * ............................................................................
 *  
 * Packet_header header file
 *
 * @author 	Vaibhav Vora <Vaibhav.Vora@technosci.com>
 * @license	{LICENSE.TXT}
 * 
 * ............................................................................
 * 
 * ----------------------------------------------------------------------------
 */

#ifndef PACKET_HEADER_H_
#define PACKET_HEADER_H_

#include <bitset>

/*
typedef struct PACKET_HEADER	 
{
	unsigned char	ReceiverType[4];
	unsigned int	PacketNumber;
	unsigned short	SATID;
	unsigned short	Year  :12; 
	unsigned char 	Month :4;
	unsigned char	Day;
	unsigned char	Hour;
	unsigned char	Minute;
	unsigned char	Sec;
	unsigned int	FracSecCounter;
	unsigned int	TuningFreq;
	unsigned short	ReceiverFlags;
	unsigned short	FracNanoSec;
}packetheader;
*/


struct FLAG_BITS {        // bits   description
   unsigned short channelNumber:3;       
   unsigned short placeholder:3;   
   unsigned short rsvd1:10;          
};

union FLAG_REG {
   unsigned short     all;
   struct FLAG_BITS    bit;
};

typedef struct PACKET_HEADER	 
{
	unsigned char	ReceiverType[4];
	unsigned int	PacketNumber;
	unsigned short	SATID;
	unsigned short	YearMonth;
	unsigned char	Hour;
	unsigned char	Day;
	unsigned char	Sec;
	unsigned char	Minute;	
	unsigned int	FiveNanoSecCount;
	unsigned int	TuningFreq;
    FLAG_REG        ReceiverFlag;    
    //std::bitset<16> ReceiverFlags;  
	//unsigned short	ReceiverFlags;
	unsigned short	Reserved;	
}packetheader;

#endif // Packet_header