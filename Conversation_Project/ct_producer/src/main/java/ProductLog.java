import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 模块：
 * 17005930322,17601615878,2019-07-01 12:38:01,1437
 * 主叫     被叫    通话时间    通话时长（秒）
 */
public class ProductLog {

    //通话时间范围限制
    String startTime = "2019-01-01";
    String endTime = "2019-12-31";


    //要有存放电话号码的集合List<String>
    List<String> phoneList = new ArrayList<>();

    //存放电话号码所对应的姓名
    //map<key,value> ---> map<电话号码，姓名>
    Map<String, String> phoneNameMap = new HashMap<>();

    //初始化电话号码以及姓名
    public void initPhone() {
        //20个随机电话
        phoneList.add("17078388295");
        phoneList.add("13980337439");
        phoneList.add("14575535933");
        phoneList.add("19902496992");
        phoneList.add("18549641558");
        phoneList.add("17005930322");
        phoneList.add("18468618874");
        phoneList.add("18576581848");
        phoneList.add("15978226424");
        phoneList.add("15542823911");
        phoneList.add("17526304161");
        phoneList.add("15422018558");
        phoneList.add("17269452013");
        phoneList.add("17764278604");
        phoneList.add("15711910344");
        phoneList.add("15714728273");
        phoneList.add("16061028454");
        phoneList.add("16264433631");
        phoneList.add("17601615878");
        phoneList.add("15897468949");

        //随机电话对应的姓名
        phoneNameMap.put("17078388295", "李雁");
        phoneNameMap.put("13980337439", "卫艺");
        phoneNameMap.put("14575535933", "仰莉");
        phoneNameMap.put("19902496992", "陶欣悦");
        phoneNameMap.put("18549641558", "施梅梅");
        phoneNameMap.put("17005930322", "金虹霖");
        phoneNameMap.put("18468618874", "魏明艳");
        phoneNameMap.put("18576581848", "华贞");
        phoneNameMap.put("15978226424", "华啟倩");
        phoneNameMap.put("15542823911", "仲采绿");
        phoneNameMap.put("17526304161", "卫丹");
        phoneNameMap.put("15422018558", "戚丽红");
        phoneNameMap.put("17269452013", "何翠柔");
        phoneNameMap.put("17764278604", "钱溶艳");
        phoneNameMap.put("15711910344", "钱琳");
        phoneNameMap.put("15714728273", "缪静欣");
        phoneNameMap.put("16061028454", "焦秋菊");
        phoneNameMap.put("16264433631", "吕访琴");
        phoneNameMap.put("17601615878", "沈丹");
        phoneNameMap.put("15897468949", "褚美丽");
    }


    /**
     * 生成数据
     * 主叫、被叫、通话时间、通话时长
     *
     * @return 返回数据
     */
    public String product() {

        //主叫和被叫电话号码
        String caller = null;
        String callee = null;

        //主叫和被叫人的姓名
        String callerName = null;
        String calleeName = null;

        //随机打电话
        //随机范围[0,1)
        //取得主叫的索引[0,20)
        int callerIndex = (int) (Math.random() * phoneList.size());

        //找到电话号码
        caller = phoneList.get(callerIndex);

        //打电话号码人的姓名
        callerName = phoneNameMap.get(caller);

        //有一个问题，万一相等了怎么办
        while (true) {
            //取得被叫
            int calleeIndex = (int) (Math.random() * phoneList.size());

            //找到电话号码
            callee = phoneList.get(calleeIndex);

            //打电话号码人的姓名
            calleeName = phoneNameMap.get(callee);

            //不相同的时候就退出循环
            if (!caller.equals(callee)) {
                break;
            }
        }

        //随机通话建立时间
        String buildTime = randomBuildTime(startTime, endTime);

        //随机通话时长
        //默认规定：打电话不超过30分钟，以秒为单位：30 * 60 = 1800 0000
        //设置通话时间的位数
        DecimalFormat df = new DecimalFormat("0000");

        //产生的结果（0,1800）
        String duration = df.format((int) (30 * 60 * Math.random()));

        //数据串拼接
        StringBuffer buffer = new StringBuffer();
        buffer.append(caller + ",").append(callee + ",").append(buildTime + ",").append(duration);

        //是buffer需要转换一个toString方式
        return buffer.toString();
    }

    private String randomBuildTime(String startTime, String endTime) {

        try {
            //进行时间格式化处理
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = null;

            startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if (endDate.getTime() <= startDate.getTime()) {
                return null;
            }

            //（结束时间 - 开始时间）* 随机数 + 开始时间
            long radomTs = (long) ((endDate.getTime() - startDate.getTime()) * Math.random()) + startDate.getTime();

            //转换一个随机时间
            Date resultDate = new Date(radomTs);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String resultTimeString = sdf2.format(resultDate);

            return resultTimeString;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //这个如果不写的话就会报错
        return null;
    }

    //把得到的数据写到文件中去
    public void writeLog(String fileName){
        try {
            //FileOutputStream 里双参构造（文件名，是否以追加的方式写入到文本）
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(fileName, true), "UTF-8");
            while (true) {
                Thread.sleep(500);
                String log = product();
                System.out.println(log);
                osw.write(log + "\n");
                osw.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {
       //实例化对象
        ProductLog log = new ProductLog();

        //初始化数据
        log.initPhone();

        //生产数据
        //16264433631,15711910344,2019-08-19 14:12:32,1394
        // 这个要写到文件流
        /*while (true) {
            log.product();
        }*/

        //打成jar包往Linux上跑
        log.writeLog(args[0]);

    }
}