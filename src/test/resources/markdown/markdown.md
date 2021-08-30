# HELLO

SCI全称是/26:Science Citation Index/，是/32,b,orange:美国科学信息研究所/（ Institute for Scientific Information，简称ISI）出版的一部世界著名的期刊文献检索工具，其出版形式包括印刷版期刊和光盘版及联机数据库，还发行了互联网上Web版数据库。SCI收录全世界出版的数、理、化、农、林、医、生命科学、天文、地理、环境、材料、工程技术等自然科学各学科的核心期刊约3500种。ISI通过它严格的选刊标准和评估程序挑选刊源，而且每年略有增减，从而做到SCI收录的文献能全面覆盖全世界最重要和最有影响力的研究成果。ISI所谓最有影响力的研究成果，指的是报道这些成果的文献大量地被其它文献引用。为此，作为一部检索工具，SCI一反其它检索工具通过主题或分类途径检索文献的常规做法，而设置了独特的“引文索引”（Citation Index）。即通过先期的文献被当前文献的引用，来说明文献之间的相关性及先前文献对当前文献的影响力。SCI以上做法上的特点，使得SCI不仅作为一部文献检索工具使用，而且成为科研评价的一种依据。科研机构被SCI收录的论文总量，反映整个机构的科研、尤其是基础研究的水平；个人的论文被SCI收录的数量及被引用次数，反映他的研究能力与学术水平。
SCI包括哪些期刊? /48,red,s:SCI是一个国际上知名权威的学术检索工具/，所检索的内容包括学术期刊以及各类专业文献，当然都是全世界范围内符合标准的期刊和文献，所以sci包括很多期刊，但sci也有一定专业侧重，sci是侧重理科专业的，因此理科专业的期刊和文献是SCI检索的主要内容。
　　理科专业中如数学、物理学、化学、天文学、生物学、医学、农业科学以及计算机科学、材料科学等学科方面重要的学术成果信息，SCI都是可以检索的，期刊是主要的形式，但SCI也不仅仅局限于期刊，一些学术会议和专利也是可以检索到的。
　　SCI检索的期刊中，需要特别提醒大家注意，特刊、增刊、会议集这些在国内认可度很有限的刊物在SCI中也是可以检索得到的，那么SCI中的特刊、增刊、会议集在国内受认可吗?在SCI中，一般特刊会被全文检索，但检索类型有所不同，一般是论著，增刊被检索的概率要小得多，大多数不检索，如果被检索，检索类型为会议文献类型。
　　所以SCI期刊是有很多类型的，涉及范围还是很广的，/12,i:作者要结合自己的发表需求检索合适的期刊/，在利用SCI检索文献时也需要多加注意。
作者观点：大家发表论文一般会向这里发表，国内已经形成了一个产业链，大家可以百度“SCI”一下。

<div>SCI全称是/26:Science Citation Index/</div>

<for in="1 to 10">
    <div style="width: 100px">大家发表论文一般会向这里发表，`国内已经形成了一个产业链`，大家可以百度“SCI”一下。</div>
</for>

```html
<script type="text/javascript" src="/root.clock.js"></script>
<script type="text/javascript" src="/root.popup.js"></script>
<link href="/css/root/clock.css" rel="stylesheet" type="text/css" />

<input id="DateTime" type="text" size="30" placeholder="yyyy-MM-dd HH:mm:00" /><a id="DateTimePicker_OpenButton" href="javascript:void(0)" style="margin-left: -24px"><i class="iconfont icon-calendar"></i></a>

<div id="DateTimePicker" popup="yes" class="popup-autosize" display="sidebar" position="right">
    <div id="DateTimePicker_CloseButton" class="popup-close-button"><i class="iconfont icon-quxiao"></i></div>
    <div class="popup-bar"><i class="iconfont icon-calendar"></i> &nbsp; <span id="DateTimePickerTitle"></span></div>
    <div class="popup-title">请分别选择日期和时间</div>
    <calendar id="Calendar" lunar="yes" corner-names="休,班" week-title="周" week-names="一,二,三,四,五,六,日" month-names="一月,二月,三月,四月,五月,六月,七月,八月,九月,十月,十一月,十二月" this-month-text="本月" today-text="今天" head-format="yyyy年M月" holiday="yes" extension-api="/api/system/calendar?year=" date="today"></calendar>
    <div class="space10"></div>
    <clock id="Clock" hour-interval="1" minute-interval="1" second-interval="0" option-frame-side="upside" value="HH:mm:00"></clock>
    <div id="DateTimePickerTip" class="space40 error" style="display: flex; justify-content: center; align-items: center;">&nbsp;</div>
    <div class="popup-button"><input id="DateTimePicker_ConfirmButton" type="button" value=" OK " class="normal-button prime-button w80" /> &nbsp; &nbsp; &nbsp; <input id="DateTimePicker_CancelButton" type="button" value=" Cancel " class="normal-button minor-button w80" /></div>
</div>

<script type="text/javascript">
$listen('DateTimePicker').on('confirm', function() {
    $x(`#DateTime`).value($calendar(`Calendar`).date + ` ` + $clock('Clock').time);
});
</script>
```