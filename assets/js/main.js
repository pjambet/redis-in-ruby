Even._initToc = function() {
    const SPACING = 20;
    const $toc = $('.post-toc');
    const $footer = $('.post-footer');

    if ($toc.length) {
      const minScrollTop = $toc.offset().top - SPACING;
      const maxScrollTop = $footer.offset().top - $toc.height() - SPACING;

      const tocState = {
        start: {
          'position': 'absolute',
          'top': minScrollTop,
        },
        process: {
          'position': 'fixed',
          'top': SPACING,
        },
        end: {
          'position': 'absolute',
          'top': maxScrollTop,
        },
      };

      $(window).scroll(function() {
        const scrollTop = $(window).scrollTop();

        if (scrollTop < minScrollTop) {
          $toc.css(tocState.start);
        } else if (scrollTop > maxScrollTop) {
          $toc.css(tocState.end);
        } else {
          $toc.css(tocState.process);
        }
      });
    }

    const HEADERFIX = 30;
    const $toclink = $('.toc-link');
    const $headerlink2 = $('.headerlink');
    const $headerlink = [];
    $headerlink2.each(function(i, a) {
      $toclink.each(function(j, a2) {
        if (a.href === a2.href) {
          $headerlink.push(a);
        }
      });
    });
    const $tocLinkLis = $('.post-toc-content li');

    const headerlinkTop = $.map($headerlink, function(link) {
      return $(link).offset().top;
    });

    const headerLinksOffsetForSearch = $.map(headerlinkTop, function(offset) {
      return offset - HEADERFIX;
    });

    const searchActiveTocIndex = function(array, target) {
      for (let i = 0; i < array.length - 1; i++) {
        if (target > array[i] && target <= array[i + 1]) return i;
      }
      if (target > array[array.length - 1]) return array.length - 1;
      return -1;
    };

    $(window).scroll(function() {
      const scrollTop = $(window).scrollTop();
      const activeTocIndex = searchActiveTocIndex(headerLinksOffsetForSearch, scrollTop);

      $($toclink).removeClass('active');
      $($tocLinkLis).removeClass('has-active');

      if (activeTocIndex !== -1) {
        $($toclink[activeTocIndex]).addClass('active');
        let ancestor = $toclink[activeTocIndex].parentNode;
        while (ancestor.tagName !== 'NAV') {
          $(ancestor).addClass('has-active');
          ancestor = ancestor.parentNode.parentNode;
        }
      }
    });
  };

  $(document).ready(function () {
    Even.backToTop();
    Even.mobileNavbar();
    Even.toc();
    Even.fancybox();

    // Hack to add a class to all the listing ... in <em> tags
    $('em:contains(listing)').addClass('listing');
  });

  Even.responsiveTable();
  Even.flowchart();
  Even.sequence();

  if (window.hljs) {
    hljs.initHighlighting();
    Even.highlight();
  } else {
    Even.chroma();
  }