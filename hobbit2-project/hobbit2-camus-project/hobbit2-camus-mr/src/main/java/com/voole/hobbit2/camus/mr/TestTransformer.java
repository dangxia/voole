/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr;

import com.voole.hobbit2.camus.api.transform.AvroCtypeKafkaTransformer;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class TestTransformer {
	public static void main(String[] args) throws TransformException {
		String msg = "3249065509265744850	1411021973	629213	1411030260	0	29206	0	0	1929807	0	10	4100331066	1	8228	14554	16421	29	526154	550135	13543	10	4100331066	1	8204	14585	32842	59	488530	616334	13350	7	4117108282	1	7731	15594	3217	28	478263	111022	12864	11	4117108282	1	7403	15966	16421	42	470156	388129	22865	8	4133885498	1	6924	17164	32842	73	432409	419363	16761	9	4133885498	1	6887	17402	32842	72	369068	467089	16048	6	934937821	1	2640	44542	0	0	179704	187398	44274	21	884606173	1	2589	47759	0	0	171098	159917	51952	25	934937821	1	2697	44097	0	0	170813	183933	44042	22	884606173	1	2333	54437	0	0	105187	106559	80932	24	0";
		AvroCtypeKafkaTransformer transformer = new AvroCtypeKafkaTransformer(
				OrderPlayAliveReqV3.getClassSchema());
		transformer.transform(msg.getBytes());
	}
}
