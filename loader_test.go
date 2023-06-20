/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @project stringman
 * @author jin.freestyle@gmail.com
 */

package stringman

import (
	"bytes"
	"encoding/xml"
	"io"
	"strings"
	"testing"
)

var testXml = []byte(`
<?xml version="1.0" encoding="UTF-8" ?>
<query>
    <text id="ClearTedTrackTemp">
        TRUNCATE ted_track_temp
    </text>

    <text id="InsertTedTrackTemp">
        INSERT INTO ted_track_temp(
            track_id,
            disc_id,
            album_id,
            track_no,
            track_title,
            len,
            title_yn,
            download_yn,
            streaming_premium_yn,
            download_premium_yn,
            pps_yn,
            ppd_yn,
            price,
            svc_192_yn,
            svc_320_yn,
            agency_id,
            db_sts,
            row_no
        ) VALUES(
            {TrackId},
            {DiscId},
            {AlbumId},
            {TrackNo},
            {TrackTitle},
            {Len},
            {TitleYn},
            {DownloadYn},
            {StreamingPremiumYn},
            {DownloadPremiumYn},
            {PpsYn},
            {ppdYn},
            {Price},
            {Svc192Yn},
            {Svc320Yn},
            {AgencyId},
            {DbSts},
            {RowNo}
        )
    </text>

    <text id="InsertTedAdult">
        INSERT INTO ted_adult (track_id)
            SELECT ? FROM DUAL
        WHERE NOT EXISTS
            (SELECT track_id FROM ted_adult WHERE track_id=?)
    </text>
    <text id="DeleteTedAdult">
        DELETE FROM ted_adult WHERE track_id = {trackId}
    </text>
    <text id="InsertTedAgency">
        INSERT INTO ted_agency (agency_id, agency_nm)
        SELECT ?, ? FROM DUAL
        WHERE NOT EXISTS
        (SELECT agency_id, agency_nm FROM ted_agency WHERE agency_id=?)
    </text>
    <text id="InsertTedAlbum">
        INSERT INTO ted_album
        (
            album_id,
            title,
            release_ymd,
            disc_cnt,
            nation_cd,
            edition_no,
            album_tp,
            album_buy_yn,
            album_buy_amt,
            db_sts
        )
        VALUES
        (
            {AlbumId},
            {Title},
            {ReleaseYmd},
            {DiscCnt},
            {NationCd},
            {EditionNo},
            {AlbumTp},
            {AlbumBuyYn},
            {AlbumBuyAmt},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            title = {Title},
            release_ymd = {ReleaseYmd},
            disc_cnt = {DiscCnt},
            nation_cd = {NationCd},
            edition_no = {EditionNo},
            album_tp = {AlbumTp},
            album_buy_yn = {AlbumBuyYn},
            album_buy_amt = {AlbumBuyAmt},
            db_sts = "A"
    </text>
    <text id="InsertTedAlbumArtist">
        INSERT INTO ted_albumartist
        (
            albumartist_id,
            artist_id,
            album_id,
            rp_yn,
            listorder,
            db_sts
        )
        VALUES
        (
            {AlbumArtistId},
            {ArtistId},
            {AlbumId},
            {RpYn},
            {Listorder},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            db_sts = "A",
            rp_yn = {RpYn},
            listorder = {Listorder}
    </text>
    <text id="InsertTedAlbumStyle">
        INSERT INTO ted_albumstyle
        (
            album_id,
            style_id,
            db_sts
        )
        VALUES
        (
            {AlbumId},
            {StyleId},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            style_id = {StyleId},
            db_sts = "A"
    </text>
    <text id="InsertTedAlbumImage">
        INSERT INTO ted_albumimage(
            album_id,
            size,
            url
        ) VALUES (
            {AlbumId},
            {Size},
            {Url}
        )
        ON DUPLICATE KEY
        UPDATE
            url = {Url}
    </text>
    <text id="InsertTbAlbumImage">
        INSERT INTO tb_album_img (
            album_id,
            chnl_type,
            album_img_size,
            album_img_url,
            create_dtime,
            update_dtime
        ) VALUES (
            {AlbumId},
            "MM",
            {Size},
            {Url},
            now(),
            now()
        )
        ON DUPLICATE KEY
        UPDATE
            album_img_url = {Url},
            update_dtime = now()
    </text>
    <text id="InsertTedArtist">
        INSERT INTO ted_artist(
            artist_id,
            artist_nm,
            grp_cd,
            sex_cd,
            act_start_ymd,
            act_end_ymd,
            nation_cd,
            db_sts
        ) VALUES (
            {ArtistId},
            {ArtistNm},
            {GrpCd},
            {SexCd},
            {ActStartYmd},
            {ActEndYmd},
            {NationCd},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            artist_nm = {ArtistNm},
            grp_cd = {GrpCd},
            sex_cd = {SexCd},
            act_start_ymd = {ActStartYmd},
            act_end_ymd = {ActEndYmd},
            nation_cd = {NationCd},
            db_sts = "A"
    </text>
    <text id="InsertTedArtistGroup">
        INSERT INTO ted_artistgroup (
            group_id,
            member_id,
            act_yn,
            db_sts
        ) VALUES(
            {GroupId},
            {MemberId},
            {ActYn},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            act_yn = {ActYn},
            db_sts = "A"
    </text>
    <text id="InsertTedArtistImage">
        INSERT INTO ted_artistimage(
            artist_id,
            size,
            url
        ) VALUES (
            {ArtistId},
            {Size},
            {Url}
        )
        ON DUPLICATE KEY
        UPDATE
            url = {Url}
    </text>
    <text id="InsertTedArtistStyle">
        INSERT INTO ted_artiststyle
        (
            artist_id,
            style_id,
            db_sts
        )
        VALUES
        (
            {ArtistId},
            {StyleId},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            db_sts = "A"
    </text>
    <text id="InsertTedCode">
        INSERT INTO ted_code
        (
            cd_id,
            cd_nm,
            db_sts
        )
        VALUES
        (
            {CodeId},
            {CodeName},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            cd_nm = {CodeName},
            db_sts = "A"
    </text>
    <text id="InsertTedCodeDtl">
        INSERT INTO ted_codedtl
        (
            cd_dtl_cd,
            cd_id,
            cd_dtl_nm,
            db_sts
        )
        VALUES
        (
            {CodeDtlCode},
            {CodeId},
            {CodeDtlName},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            cd_dtl_nm = {CodeDtlName},
            db_sts = "A"
    </text>
    <text id="InsertTedDisc">
        INSERT INTO ted_disk
        (
            album_id,
            disc_id,
            disc_no,
            db_sts
        )
        VALUES
        (
            {AlbumId},
            {DiscId},
            {DiscNo},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            disc_id = {DiscId},
            disc_no = {DiscNo},
            db_sts = "A"
    </text>
    <text id="InsertTedStyle">
        INSERT INTO ted_style
        (
            genre_id,
            style_id,
            style_nm,
            db_sts
        )
        VALUES
        (
            {GenreId},
            {StyleId},
            {StyleName},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            style_nm = {StyleName},
            db_sts = "A"
    </text>
    <text id="InsertTedTrackArtist">
        INSERT INTO ted_trackartist
        (
            trackartist_id,
            artist_id,
            track_id,
            rp_yn,
            listorder,
            role_cd,
            db_sts
        )
        VALUES
        (
            {TrackArtistId},
            {ArtistId},
            {TrackId},
            {RpYn},
            {Listorder},
            {RoleCd},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            artist_id = {ArtistId},
            track_id = {TrackId},
            rp_yn = {RpYn},
            listorder = {Listorder},
            role_cd = {RoleCd},
            db_sts = "A"
    </text>
    <text id="InsertTedTrack">
        INSERT INTO ted_track (
            track_id,
            disc_id,
            album_id,
            track_no,
            track_title,
            len,
            title_yn,
            download_yn,
            streaming_premium_yn,
            download_premium_yn,
            pps_yn,
            ppd_yn,
            price,
            svc_192_yn,
            svc_320_yn,
            agency_id,
            db_sts
        ) VALUES(
            {TrackId},
            {DiscId},
            {AlbumId},
            {TrackNo},
            {TrackTitle},
            {Len},
            {TitleYn},
            {DownloadYn},
            {StreamingPremiumYn},
            {DownloadPremiumYn},
            {PpsYn},
            {PpdYn},
            {Price},
            {Svc192Yn},
            {Svc320Yn},
            {AgencyId},
            {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            disc_id = {DiscId},
            album_id = {AlbumId},
            track_no = {TrackNo},
            track_title = {TrackTitle},
            len = {Len},
            title_yn = {TitleYn},
            download_yn = {DownloadYn},
            streaming_premium_yn = {StreamingPremiumYn},
            download_premium_yn = {DownloadPremiumYn},
            pps_yn = {PpsYn},
            ppd_yn = {PpdYn},
            price = {Price},
            svc_192_yn = {Svc192Yn},
            svc_320_yn = {Svc320Yn},
            agency_id = {AgencyId},
            db_sts = "A"
    </text>
    <text id="DeleteTedAlbum">
        DELETE FROM ted_album WHERE album_id = {AlbumId}
    </text>
    <text id="DeleteTedAlbumArtist">
        DELETE FROM ted_albumartist WHERE albumartist_id = {AlbumArtistId}
    </text>
    <text id="DeleteTedAlbumStyle">
        DELETE FROM ted_albumstyle WHERE album_id = {AlbumId}
    </text>
    <text id="DeleteTedArtist">
        DELETE FROM ted_artist WHERE artist_id = {ArtistId}
    </text>
    <text id="DeleteTedArtistGroup">
        DELETE FROM ted_artistgroup WHERE group_id = {GroupId}
    </text>
    <text id="DeleteTedArtistStyle">
        DELETE FROM ted_albumstyle WHERE album_id = {ArtistId}
    </text>
    <text id="DeleteTedCode">
        DELETE FROM ted_code WHERE cd_id = {CodeId}
    </text>
    <text id="DeleteTedCodeDtl">
        DELETE FROM ted_codedtl WHERE cd_dtl_cd = {CodeDtlCode}
    </text>
    <text id="DeleteTedDisc">
        DELETE FROM ted_disk WHERE album_id = {AlbumId}
    </text>
    <text id="DeleteTedStyle">
        DELETE FROM ted_style WHERE genre_id = {GenreId} AND style_id = {StyleId}
    </text>
    <text id="DeleteTedTrackArtist">
        DELETE FROM ted_trackartist WHERE trackartist_id = {TrackArtistId}
    </text>
    <text id="DeleteTedTrack">
        DELETE FROM ted_track WHERE track_id = {TrackId}
    </text>

    <text id="SelectTedTrackTemp">
        SELECT
            track_id,
            disc_id,
            album_id,
            track_no,
            track_title,
            len,
            title_yn,
            download_yn,
            streaming_premium_yn,
            download_premium_yn,
            pps_yn,
            ppd_yn,
            price,
            svc_192_yn,
            svc_320_yn,
            agency_id,
            db_sts
        FROM ted_track_temp ORDER BY row_no ASC
    </text>
    <text id="SelectTedTrackList">
        SELECT
            a.track_id				AS track_id,
            a.disc_id               AS disc_id,
            a.track_title			AS track_nm,
            a.album_id				AS album_id,
            a.track_no              AS track_no,
            a.len					AS track_play_tm,
            CASE
            <![CDATA[
            WHEN b.track_id is not null  THEN 'Y'  ELSE 'N'
            ]]>
            END 					AS adult_auth_need_track_yn,
            a.streaming_premium_yn	AS streaming_premium_yn,
            a.pps_yn				AS pps_yn,
            'Y'						AS disp_status_yn,
            1						AS track_subtract_qty,
            0						AS track_popularity,
            now()					AS create_dtime,
            now()					AS update_dtime,
            CASE
            WHEN a.title_yn = NULL THEN 'N'
            WHEN a.title_yn = '' THEN 'N'
            ELSE a.title_yn
            END 					AS title_yn,
            a.agency_id             AS agency_id,
            a.db_sts				AS db_sts
        FROM ted_track_temp A LEFT JOIN ted_adult B
        ON A.track_id = B.track_id
    </text>


    <text id="UpdateTbTrack">
        UPDATE tb_track
        SET
            disp_status_yn = 'N',
            update_dtime = now()
        WHERE
            track_id = {trackId}
    </text>

    <text id="SelectChnlIdFromMap">
        SELECT DISTINCT(chnl_id) FROM tb_map_chnl_track WHERE track_id IN ({TrackIdList})
    </text>

    <text id="UpdateTbTrackCount">
        <![CDATA[
update tb_chnl c , (
        select
            b.chnl_id chnl_id         ,
            sum( b.track_cnt ) track_cnt         ,
            case
                when length( cast( sum( b.track_tm_sec ) /60 as unsigned )  ) <= 2             then lpad( cast( sum( b.track_tm_sec ) /60 as unsigned )  ,
                2,
                '0')
                else cast( sum( b.track_tm_sec ) /60 as unsigned )
            end as track_tm_min
        from
            (         select
                m.chnl_id                 ,
                1 track_cnt                 ,
                substr(track_play_tm ,
                1,
                2 ) * 60 + substr(track_play_tm,
                4,
                2 ) track_tm_sec
            from
                tb_track t
            join
                tb_map_chnl_track m
                    on t.track_id = m.track_id
            where
                t.disp_status_yn = 'Y' AND m.chnl_id={ChnlId}    ) b
        group by
            b.chnl_id ) d
        set
            c.track_cnt = d.track_cnt     ,
            c.chnl_play_tm = d.track_tm_min
        where
            c.chnl_id = d.chnl_id AND c.chnl_id={ChnlId}
]]>
    </text>


    <text id="SelectLyricsTrackExist">
        SELECT count(track_id)
        FROM tb_track
        WHERE track_id = {TrackId}
    </text>

    <text id="DeleteTedLyrics">
        DELETE FROM ted_lyrics WHERE track_id = {TrackId}
    </text>

    <text id="InsertTedLyrics">
        INSERT INTO ted_lyrics (
            track_id, lyrics_tp, lyrics, db_sts
        ) VALUES (
            {TrackId}, {LyricsTp}, {Lyrics}, {DbSts}
        )
        ON DUPLICATE KEY
        UPDATE
            db_sts = "A",
            lyrics_tp = {LyricsTp},
            lyrics = {Lyrics}
    </text>

    <text id="InsertTbTrack">
        INSERT INTO tb_track (
            track_id,
            track_nm,
            album_id,
            disk_id,
            track_no,
            track_play_tm,
            adult_auth_need_track_yn,
            streaming_premium_yn,
            pps_yn,
            disp_status_yn,
            agency_id,
            track_subtrct_qty,
            track_popularity,
            create_dtime,
            update_dtime,
            title_yn
        ) VALUES (
            {trackId},
            {trackName},
            {albumId},
            {diskId},
            {trackNo},
            {trackPlayTime},
            {adultAuthNeedTrackYn},
            {streamingPremiumYn},
            {ppsYn},
            {displayStatusYn},
            {agencyId},
            {trackSubtractQuantity},
            {trackPopularity},
            now(),
            now(),
            {titleYn}
        )
        ON DUPLICATE KEY UPDATE
            track_nm = {trackName},
            album_id = {albumId},
            disk_id = {diskId},
            track_no = {trackNo},
            track_play_tm = {trackPlayTime},
            adult_auth_need_track_yn = {adultAuthNeedTrackYn},
            streaming_premium_yn = {streamingPremiumYn},
            pps_yn = {ppsYn},
            disp_status_yn = {displayStatusYn},
            agency_id = {agencyId},
            track_subtrct_qty = {trackSubtractQuantity},
            track_popularity = {trackPopularity},
            title_yn = {titleYn},
            update_dtime = now()
    </text>

    <text id="SelectChnlIdWithTrackId">
        SELECT DISTINCT(chnl_id) FROM tb_map_chnl_track WHERE track_id={TrackId}
    </text>

    <text id="UpdateChnlImage">
        UPDATE    tb_chnl a
            JOIN tb_map_chnl_track b ON b.chnl_id = a.chnl_id
            JOIN (
                SELECT mt.chnl_id, min(track_sn) track_sn
                FROM tb_map_chnl_track mt
                JOIN tb_track tt ON tt.track_id = mt.track_id
                WHERE tt.disp_status_yn = 'Y' AND mt.chnl_id = {ChnlId}
                GROUP BY mt.chnl_id ) f ON b.chnl_id = f.chnl_id AND b.track_sn = f.track_sn
        JOIN tb_track t ON t.track_id = b.track_id
        SET a.album_id = t.album_id
        WHERE t.disp_status_yn = 'Y' AND a.chnl_id={ChnlId}
    </text>
</query>
`)

func TestLoaderComplicated(t *testing.T) {
	queryNormalizer = newNormalizer()

	stmtList = make([]QueryStatement, 0)
	buf := bytes.NewBuffer(testXml)
	dec := xml.NewDecoder(buf)

	for {
		t, tokenErr := dec.Token()
		if tokenErr != nil {
			if tokenErr == io.EOF {
				break
			}
			panic(tokenErr)
		}

		switch t := t.(type) {
		case xml.StartElement:
			currentId = getAttr(t.Attr, attrId)
			currentEleType = buildElementType(t.Name.Local)
			if currentEleType.IsText() {
				currentStmt = newQueryStatement()
				traverseIf(dec)
			}
		case xml.CharData:
			if len(currentId) == 0 {
				break
			}
			currentStmt.Query = currentStmt.Query + string(t)
		case xml.EndElement:
			if currentEleType.IsText() {
				currentStmt.Query = strings.Trim(currentStmt.Query, cutset)
				currentId = ""
			}
		}
	}

	if len(stmtList) != 43 {
		t.Errorf("expect stmt len 43. %d", len(stmtList))
	}

}
