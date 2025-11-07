// just some datatypes (Site and Page) from DotNetWikiBot to keep existing code compiling for now.
// see DotNetWikiBot.cs for details

using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;

namespace DotNetWikiBot {

    /// <summary>Class defines wiki site object.</summary>
    [Serializable]
    public class Site {
        /// <summary>Site's URI.</summary>
        public string address;
        /// <summary>User's account to login with.</summary>
        public string userName;
        /// <summary>User's password to login with.</summary>
        public string userPass;
        /// <summary>Default domain for LDAP authentication, if such authentication is allowed on
        /// this site. Additional information can be found at
        /// http://www.mediawiki.org/wiki/Extension:LDAP_Authentication </summary>
        public string userDomain = "";
        /// <summary>Site title, e.g. "Wikipedia".</summary>
        public string name;
        /// <summary>Site's software identificator, e.g. "MediaWiki 1.21".</summary>
        public string software;
        /// <summary>MediaWiki version as number.</summary>
        public double versionNumber;
        /// <summary>MediaWiki version as Version object.</summary>
        public Version version;
        /// <summary>If set to false, bot will use MediaWiki's common user interface where
        /// possible, instead of using special API interface for robots (api.php). Default is true.
        /// Set it to false manually if some problem with API interface arises on site.</summary>
        public bool useApi = true;
        /// <summary>Mandatory page title capitalization rules on this site. Most commonly
        /// this variable equals "first-letter" string.</summary>
        public string capitalization;
        /// <summary>Site's time offset from UTC.</summary>
        public string timeOffset;
        /// <summary>Absolute path to MediaWiki's "index.php" file on the server.</summary>
        public string indexPath;
        /// <summary>Absolute path to MediaWiki's "api.php" file on the server.</summary>
        public string apiPath;
        /// <summary>Short relative path to wiki pages (if such alias is set on the server), e.g.
        /// "/wiki/". See "http://www.mediawiki.org/wiki/Manual:Short URL" for details.</summary>
        public string shortPath;

        /// <summary>User's watchlist. This PageList is not filled automatically when Site object
        /// is constructed, you need to call FillFromWatchList function to fill it.</summary>
        //public PageList watchList;

        /// <summary>MediaWiki system messages (those listed on "Special:Allmessages" page),
        /// user-modified versions. This dictionary is not filled automatically when Site object
        /// is constructed, you need to call LoadMediawikiMessages(true) function to load messages
        /// into this dictionary.</summary>
        public Dictionary<string, string> messages;
        /// <summary>Default edit comment. You can set it to whatever you would like.</summary>
        /// <example><code>mySite.defaultEditComment = "My default edit comment";</code></example>
        public string defaultEditComment = "Automatic page editing by robot";
        /// <summary>If set to true, all bot's edits are marked as minor by default.</summary>
        public bool minorEditByDefault = true;
        /// <summary>Number of times to retry bot web action in case of temporary connection
        ///  failure or some server problems.</summary>
        public int retryTimes = 3;
        /// <summary>Number of list items to fetch at a time. This settings concerns special pages
        /// output and API lists output. Default is 500. Bot accounts are allowed to fetch
        /// up to 5000 items at a time. Adjust this number if required.</summary>
        public int fetchRate = 500;
        /// <summary>Templates, which are used to distinguish disambiguation pages. Set this
        /// variable manually if required. Multiple templates can be specified, use '|'
        /// character as the delimeter. Letters case doesn't matter.</summary>
        /// <example><code>site.disambig = "disambiguation|disambig|disam";</code></example>
        public string disambig;
        /// <summary>A set of regular expressions for internal functions. Usually there is no need
        /// to edit these regular expressions manually.</summary>
        public Dictionary<string, Regex> regexes = new Dictionary<string, Regex>();
        /// <summary>Site's cookies.</summary>
        public CookieContainer cookies = new CookieContainer();
        /// <summary>XML name table for parsing XHTML documents.</summary>
        public NameTable xhtmlNameTable = new NameTable();
        /// <summary>XML namespace manager for parsing XHTML documents.</summary>
        public XmlNamespaceManager xmlNs;
        /// <summary>Local namespaces, default namespaces and local namespace aliases, joined into
        /// strings, enclosed in and delimited by '|' character.</summary>
        public Dictionary<int, string> namespaces;
        /// <summary>Supplementary data, mostly localized strings.</summary>
        public XElement generalDataXml;
        /// <summary>Parsed supplementary data, mostly localized strings.</summary>
        public Dictionary<string, string> generalData;
        /// <summary>Site's language.</summary>
        public string language;
        /// <summary>Site's neutral (language) culture. Required for string comparison.</summary>
        public CultureInfo langCulture;
        /// <summary>Randomly chosen regional (non-neutral) culture for site's language.
        /// Required to parse dates.</summary>
        public CultureInfo regCulture;
    }

    /// <summary>Class defines wiki page object.</summary>
    [Serializable]
    public class Page {
        /// <summary>Page's title, including namespace prefix.</summary>
        public string title;
        /// <summary>Page's text.</summary>
        public string text;
        /// <summary>Site, on which this page is located.</summary>
        public Site site;
        /// <summary>Page's ID in MediaWiki database.</summary>
        public string pageId;
        /// <summary>Username or IP-address of last page contributor.</summary>
        public string lastUser;
        /// <summary>Last contributor's ID in MediaWiki database.</summary>
        public string lastUserId;
        /// <summary>Page revision ID in the MediaWiki database.</summary>
        public string revision;
        /// <summary>True, if last edit was minor edit.</summary>
        public bool lastMinorEdit;
        /// <summary>Number of bytes modified during last edit.</summary>
        public int lastBytesModified;
        /// <summary>Last edit comment.</summary>
        public string comment;
        /// <summary>Date and time of last edit expressed in UTC (Coordinated Universal Time).
        /// Call "timestamp.ToLocalTime()" to convert to local time if it is necessary.</summary>
        public DateTime timestamp;
        /// <summary>True, if this page is in bot account's watchlist.</summary>
        public bool watched;

        /// <summary>This constructor creates Page object with specified title and specified
        /// Site object. This is preferable constructor. Basic title normalization occurs during
        /// construction.
        /// When constructed, new Page object doesn't contain text, use Load() method to get text
        /// from live wiki or use LoadWithMetadata() to get both text and metadata.</summary>
        /// <param name="site">Site object, it must be constructed beforehand.</param>
        /// <param name="title">Page title as string.</param>
        /// <returns>Returns Page object.</returns>
        public Page(Site site, string title) {
            if (string.IsNullOrEmpty(title))
                throw new ArgumentNullException("title");
            if (title[0] == ':')
                title = title.TrimStart(new char[] { ':' });
            if (title.Contains('_'))
                title = title.Replace('_', ' ');

            this.site = site;
            this.title = title;

            /* // RESERVED, may interfere user intentions
			int ns = GetNamespace();
			RemoveNsPrefix();
			if (site.capitalization == "first-letter")
				title = Bot.Capitalize(title);
			title = site.namespaces[ns] + title;
			*/
        }

        /// <summary>This constructor creates empty Page object with specified Site object,
        /// but without title. Avoid using this constructor needlessly.</summary>
        /// <param name="site">Site object, it must be constructed beforehand.</param>
        /// <returns>Returns Page object.</returns>
        public Page(Site site) {
            this.site = site;
        }

        public void Load() {
            throw new NotImplementedException("Load method not implemented (this class has been gutted; use another library)");
        }

        internal IEnumerable<string> GetTemplates(bool withParameters, bool includePages) { 
            throw new NotImplementedException();
        }
    }
}